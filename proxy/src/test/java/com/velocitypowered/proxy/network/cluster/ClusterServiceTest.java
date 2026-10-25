/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.network.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.event.network.NetworkPlayerJoinEvent;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.network.cluster.ClusterService.LoginResult;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Presence rules with several proxies sharing one in-memory store.
 */
class ClusterServiceTest {

  private final AtomicLong skew = new AtomicLong();
  private final LocalStore store = new LocalStore(() -> System.currentTimeMillis() + skew.get());
  private final List<ClusterService> services = new ArrayList<>();

  /** A proxy's side: its players and the events it saw. */
  private final class FakeProxy implements ClusterService.Host {
    final Map<UUID, Long> players = new ConcurrentHashMap<>();
    final List<Object> events = new CopyOnWriteArrayList<>();
    final Map<String, Integer> servers = new ConcurrentHashMap<>();
    ClusterService service;

    @Override
    public Map<String, Integer> localServerCounts() {
      return Map.copyOf(servers);
    }

    @Override
    public int localPlayerCount() {
      return players.size();
    }

    @Override
    public java.util.Collection<String> localServers() {
      return List.of();
    }

    @Override
    public void disconnectDuplicate(UUID player) {
      Long session = players.remove(player);
      if (session != null) {
        service.playerLeft(player, session); // what ConnectedPlayer.teardown does
      }
    }

    @Override
    public void fire(Object event) {
      events.add(event);
    }

    LoginResult login(UUID uuid, String name) throws Exception {
      LoginResult r = service.claimLogin(uuid, name, "192.0.2.9", "[]").get(20, TimeUnit.SECONDS);
      if (r.denyReason() == null) {
        players.put(uuid, r.session());
      }
      return r;
    }
  }

  private FakeProxy proxy(String id, String duplicateLogin, String downPolicy) {
    FakeProxy p = new FakeProxy();
    NetworkConfig config = new NetworkConfig(true, id, "", "", "local", duplicateLogin,
        downPolicy, "", 5, 120, "", 25565, 50, secret(), true, 21600, 300, false, false, 43200);
    p.service = new ClusterService(config, id, store, p);
    p.service.start();
    services.add(p.service);
    return p;
  }

  private FakeProxy proxy(String id) {
    return proxy(id, "newest-wins", "stop-logins");
  }

  @TempDir
  static Path keyDir;

  private static String secret() {
    return keyDir.resolve("network-secret.key").toString();
  }

  @AfterEach
  void stop() {
    services.forEach(ClusterService::close);
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long end = System.currentTimeMillis() + 5_000;
    while (!condition.getAsBoolean()) {
      assertTrue(System.currentTimeMillis() < end, "timed out");
      Thread.sleep(20);
    }
  }

  private Optional<String> owner(UUID uuid) {
    return store.player(uuid).join().map(PlayerRecord::proxy);
  }

  @Test
  void loginClaimsAndQuitReleases() throws Exception {
    FakeProxy a = proxy("a");
    UUID steve = UUID.randomUUID();
    LoginResult r = a.login(steve, "Steve");
    assertNull(r.denyReason());
    assertEquals(Optional.of("a"), owner(steve));
    assertTrue(a.events.get(0) instanceof NetworkPlayerJoinEvent);

    a.service.serverChanged(steve, r.session(), "lobby");
    assertEquals(Optional.of("lobby"), store.player(steve).join().map(PlayerRecord::server));

    a.disconnectDuplicate(steve);
    assertEquals(Optional.empty(), owner(steve));
  }

  @Test
  void newestWinsAcrossProxies() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    long start = System.currentTimeMillis();
    assertNull(b.login(steve, "Steve").denyReason());
    assertTrue(System.currentTimeMillis() - start < 2_000);
    assertTrue(a.players.isEmpty());
    assertEquals(Optional.of("b"), owner(steve));
  }

  @Test
  void denyNewAcrossProxies() throws Exception {
    FakeProxy a = proxy("a", "deny-new", "stop-logins");
    FakeProxy b = proxy("b", "deny-new", "stop-logins");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    assertEquals(ClusterService.ALREADY_CONNECTED, b.login(steve, "Steve").denyReason());
    assertEquals(Optional.of("a"), owner(steve));
  }

  @Test
  void deadProxyPlayersCanLogInAtOnce() throws Exception {
    UUID steve = UUID.randomUUID();
    store.tick("crashed", 1, 15_000, "x").join();
    store.claim(new PlayerRecord(steve, "Steve", "crashed", 1, "lobby", "192.0.2.4", 0), 1, 5_000)
        .join();
    FakeProxy b = proxy("b");
    skew.addAndGet(ClusterService.DEAD_AFTER_MILLIS + 1_000); // "crashed" went silent
    assertNull(b.login(steve, "Steve").denyReason());
    assertEquals(Optional.of("b"), owner(steve));
    // b's tick sweeps the dead proxy
    await(() -> store.tick("probe", 0, 15_000, "x").join().proxies().stream()
        .noneMatch(p -> p.id().equals("crashed")));
  }

  @Test
  void restartWithSameIdDropsLeftovers() throws Exception {
    UUID steve = UUID.randomUUID();
    store.tick("a", 1, 15_000, "x").join();
    store.claim(new PlayerRecord(steve, "Steve", "a", 1, null, "192.0.2.4", 0), 1, 5_000).join();
    proxy("a"); // same id, previous run crashed
    assertEquals(Optional.empty(), owner(steve));
  }

  @Test
  void networkCountSeenEverywhereWithinOneSecond() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    a.login(UUID.randomUUID(), "One");
    a.login(UUID.randomUUID(), "Two");
    b.login(UUID.randomUUID(), "Three");
    long start = System.currentTimeMillis();
    await(() -> a.service.getPlayerCount() == 3 && b.service.getPlayerCount() == 3);
    assertTrue(System.currentTimeMillis() - start < 1_000);
  }

  @Test
  void serverCountsReachOtherProxies() throws Exception {
    final FakeProxy a = proxy("a");
    final FakeProxy b = proxy("b");
    final FakeProxy c = proxy("c");
    a.servers.put("lobby", 3);
    a.servers.put("survival", 1);
    b.servers.put("lobby", 2);
    await(() -> c.service.remotePlayerCount("lobby") == 5
        && c.service.remotePlayerCount("SURVIVAL") == 1);
    await(() -> a.service.remotePlayerCount("lobby") == 2); // others only
    assertEquals(0, c.service.remotePlayerCount("creative"));
    // plain server names stay what forwarding uses
    assertTrue(c.service.liveRemotes().stream().filter(p -> p.id().equals("a")).findFirst()
        .orElseThrow().servers().containsAll(List.of("lobby", "survival")));
    a.servers.clear();
    await(() -> c.service.remotePlayerCount("lobby") == 2);
  }

  @Test
  void networkEventsFireOnceEverywhere() throws Exception {
    final FakeProxy a = proxy("a");
    final FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    LoginResult r = a.login(steve, "Steve");
    a.service.serverChanged(steve, r.session(), "lobby");
    a.service.serverChanged(steve, r.session(), "survival");
    a.disconnectDuplicate(steve);
    await(() -> b.events.size() >= 4);
    Thread.sleep(300); // nothing arrives twice
    for (FakeProxy p : List.of(a, b)) {
      assertEquals(List.of("NetworkPlayerJoinEvent", "NetworkPlayerServerChangeEvent",
          "NetworkPlayerServerChangeEvent", "NetworkPlayerQuitEvent"),
          p.events.stream().map(e -> e.getClass().getSimpleName()).toList());
    }
    var second = (com.velocitypowered.api.event.network.NetworkPlayerServerChangeEvent)
        b.events.get(2);
    assertEquals(Optional.of("lobby"), second.getPreviousServer());
    assertEquals("b", b.service.proxyId());
    assertEquals("a", ((com.velocitypowered.api.event.network.NetworkPlayerQuitEvent)
        b.events.get(3)).getPlayer().getProxyId());
  }

  @Test
  void resyncAfterStoreLostItsData() throws Exception {
    FakeProxy a = proxy("a");
    UUID steve = UUID.randomUUID();
    LoginResult r = a.login(steve, "Steve");
    a.service.serverChanged(steve, r.session(), "lobby");
    store.wipe();
    await(() -> owner(steve).isPresent());
    assertEquals(Optional.of("lobby"), store.player(steve).join().map(PlayerRecord::server));
  }

  @Test
  void resyncLeavesOneCopyAfterDataLoss() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    store.wipe(); // the store came back empty and the player logs in on b at once
    assertNull(b.login(steve, "Steve").denyReason());
    // Whichever order the resync and the login ran in, exactly one copy stays and owns it.
    await(() -> a.players.containsKey(steve) != b.players.containsKey(steve)
        && owner(steve).equals(Optional.of(a.players.containsKey(steve) ? "a" : "b")));
    Thread.sleep(1_000); // and it stays that way
    assertTrue(a.players.containsKey(steve) != b.players.containsKey(steve));
    assertEquals(Optional.of(a.players.containsKey(steve) ? "a" : "b"), owner(steve));
  }

  @Test
  void loginWaitsOutShortOutage() throws Exception {
    FakeProxy a = proxy("a");
    store.setDown(true);
    Thread.ofPlatform().start(() -> {
      try {
        Thread.sleep(1_000);
      } catch (InterruptedException ignored) {
        // test ends anyway
      }
      store.setDown(false);
    });
    assertNull(a.login(UUID.randomUUID(), "Steve").denyReason());
  }

  @Test
  void stopLoginsWhenTheStoreStaysDown() throws Exception {
    FakeProxy a = proxy("a");
    store.setDown(true);
    assertEquals(ClusterService.UNAVAILABLE, a.login(UUID.randomUUID(), "Steve").denyReason());
  }

  @Test
  void localOnlyAdmitsAndChecksLater() throws Exception {
    FakeProxy a = proxy("a", "newest-wins", "local-only");
    store.setDown(true);
    UUID steve = UUID.randomUUID();
    assertNull(a.login(steve, "Steve").denyReason());
    store.setDown(false);
    await(() -> owner(steve).isPresent());
  }

  private FakeProxy rateProxy(String id, boolean on, int max) {
    FakeProxy p = new FakeProxy();
    p.service = new ClusterService(new NetworkConfig(true, id, "", "", "local", "newest-wins",
        "stop-logins", "", 5, 120, "", 25565, 50, secret(), true, 21600, 300, false, false,
        43200, on, max, 60), id, store, p);
    p.service.start();
    services.add(p.service);
    return p;
  }

  @Test
  void loginRateLimitIsSharedBetweenProxies() throws Exception {
    FakeProxy a = rateProxy("a", true, 3);
    FakeProxy b = rateProxy("b", true, 3);
    assertTrue(a.service.allowLogin("192.0.2.1").get());
    assertTrue(b.service.allowLogin("192.0.2.1").get());
    assertTrue(a.service.allowLogin("192.0.2.1").get());
    assertFalse(b.service.allowLogin("192.0.2.1").get());
    assertFalse(a.service.allowLogin("192.0.2.1").get());
    assertTrue(a.service.allowLogin("192.0.2.2").get()); // other addresses are not affected
    assertEquals(2, a.service.stats().loginsLimited() + b.service.stats().loginsLimited());
  }

  @Test
  void rateKeyGroupsAddresses() {
    assertEquals("192.0.2.1", ClusterService.rateKey("192.0.2.1"));
    assertEquals("192.0.2.1", ClusterService.rateKey("::ffff:192.0.2.1"));
    String net = ClusterService.rateKey("2001:db8:1:2:aaaa:bbbb:cccc:dddd");
    assertEquals(net, ClusterService.rateKey("2001:db8:1:2::1%eth0"));
    assertFalse(net.equals(ClusterService.rateKey("2001:db8:1:3::1")));
  }

  @Test
  void loginRateLimitWindowExpires() throws Exception {
    FakeProxy a = rateProxy("a", true, 1);
    assertTrue(a.service.allowLogin("192.0.2.1").get());
    assertFalse(a.service.allowLogin("192.0.2.1").get());
    skew.addAndGet(61_000);
    assertTrue(a.service.allowLogin("192.0.2.1").get());
  }

  @Test
  void loginRateLimitOffAllowsEverything() throws Exception {
    FakeProxy a = rateProxy("a", false, 1);
    for (int i = 0; i < 5; i++) {
      assertTrue(a.service.allowLogin("192.0.2.1").get());
    }
  }

  @Test
  void loginRateLimitFailsOpenWhenTheStoreIsDown() throws Exception {
    FakeProxy a = rateProxy("a", true, 1);
    assertTrue(a.service.allowLogin("192.0.2.1").get());
    store.setDown(true);
    for (int i = 0; i < 3; i++) {
      assertTrue(a.service.allowLogin("192.0.2.1").get());
    }
    store.setDown(false);
  }

  @Test
  void secondLiveProxyWithSameIdRefusesToStart() {
    proxy("a");
    FakeProxy twin = new FakeProxy();
    twin.service = new ClusterService(new NetworkConfig(true, "a", "", "", "local", "newest-wins",
        "stop-logins", "", 5, 120, "", 25565, 50, secret(), true, 21600, 300, false, false, 43200), "a", store,
        twin);
    assertThrows(IllegalStateException.class, twin.service::start);
  }

  @Test
  void idClashAtRuntimeStopsLogins() throws Exception {
    FakeProxy a = proxy("a");
    assertTrue(a.service.isReady());
    store.tick("a", 0, List.of(), "intruder", false, 15_000, "x").join();
    await(() -> !a.service.isReady());
    assertEquals(ClusterService.UNAVAILABLE, a.login(UUID.randomUUID(), "Steve").denyReason());
  }

  @Test
  void notReadyAfterTheGraceUnderStopLogins() throws Exception {
    FakeProxy a = proxy("a");
    final FakeProxy b = proxy("b", "newest-wins", "local-only");
    assertNull(a.service.readyProblem());
    store.setDown(true);
    Thread.sleep(ClusterService.READY_GRACE_MILLIS + 1_000);
    assertEquals("network store unreachable", a.service.readyProblem());
    assertNull(b.service.readyProblem()); // local-only keeps taking players
    store.setDown(false);
    await(() -> a.service.readyProblem() == null);
  }

  @Test
  void drainingIsPublished() throws Exception {
    FakeProxy a = proxy("a");
    a.service.setDraining(true);
    await(() -> store.proxy("a").join().map(NetworkStore.ProxyState::draining).orElse(false));
  }

  @Test
  void transferTicketRoutesToTheSameServerOnce() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    byte[] ticket = a.service.issueTicket(steve, "Steve", "lobby", "192.0.2.9");
    a.service.releaseForTransfer(steve).join();
    assertTrue(b.service.redeem(ticket, "192.0.2.9", "Steve", steve).join().isPresent());
    assertEquals("lobby", b.login(steve, "Steve").route());
    // the same ticket again (replay) is refused
    assertTrue(b.service.redeem(ticket, "192.0.2.9", "Steve", steve).join().isEmpty());
  }

  @Test
  void ticketDuringStoreOutageRoutesButDoesNotReplaceMojang() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    byte[] ticket = a.service.issueTicket(steve, "Steve", "lobby", "192.0.2.9");
    store.setDown(true);
    var redeemed = b.service.redeem(ticket, "192.0.2.9", "Steve", steve).join();
    store.setDown(false);
    assertTrue(redeemed.isPresent());
    assertFalse(redeemed.get().nonceChecked());
    assertEquals("lobby", b.login(steve, "Steve").route());
  }

  @Test
  void ticketOnlyForItsPlayerAndAddress() {
    FakeProxy a = proxy("a");
    UUID steve = UUID.randomUUID();
    byte[] ticket = a.service.issueTicket(steve, "Steve", "lobby", "192.0.2.9");
    assertTrue(a.service.redeem(ticket, "192.0.2.66", "Steve", steve).join().isEmpty());
    assertTrue(a.service.redeem(ticket, "192.0.2.9", "Alex", steve).join().isEmpty());
    assertTrue(a.service.redeem(ticket, "192.0.2.9", "Steve", UUID.randomUUID()).join()
        .isEmpty());
    assertTrue(a.service.redeem(null, "192.0.2.9", "Steve", steve).join().isEmpty());
    // the rejections above did not use the nonce up
    assertTrue(a.service.redeem(ticket, "192.0.2.9", "Steve", steve).join().isPresent());
  }

  @Test
  void routeHintWithoutCookie() throws Exception {
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    store.putRoute(steve, "survival", 60_000).join();
    assertEquals("survival", b.login(steve, "Steve").route());
    assertNull(b.login(UUID.randomUUID(), "Alex").route());
  }

  @Test
  void crashedProxyPlayersGoBackToTheirServer() throws Exception {
    UUID steve = UUID.randomUUID();
    store.tick("crashed", 1, 15_000, "x").join();
    store.claim(new PlayerRecord(steve, "Steve", "crashed", 1, "skyblock", "192.0.2.4", 0), 1,
        5_000).join();
    FakeProxy b = proxy("b");
    skew.addAndGet(ClusterService.DEAD_AFTER_MILLIS + 1_000);
    assertEquals("skyblock", b.login(steve, "Steve").route());
  }

  private FakeProxy outageProxy(String id, boolean on, int windowSeconds) {
    FakeProxy p = new FakeProxy();
    p.service = new ClusterService(new NetworkConfig(true, id, "", "", "local", "newest-wins",
        "stop-logins", "", 5, 120, "", 25565, 50, secret(), true, 21600, 300, false, on,
        windowSeconds), id, store, p);
    p.service.start();
    services.add(p.service);
    return p;
  }

  @Test
  void outageModeLetsKnownPlayersInFromTheSameAddress() throws Exception {
    FakeProxy a = outageProxy("a", true, 600);
    UUID steve = UUID.randomUUID();
    a.service.recordGoodLogin(steve, "Steve", "192.0.2.5");
    store.putProfile(new NetworkStore.CachedProfile(steve, "Steve",
        "[{\"name\":\"textures\",\"value\":\"v\",\"signature\":\"s\"}]"), 60_000).join();
    var profile = a.service.outageLogin("steve", "192.0.2.5").join();
    assertTrue(profile.isPresent());
    assertEquals(steve, profile.get().getId());
    assertEquals("Steve", profile.get().getName());
    assertEquals("textures", profile.get().getProperties().get(0).getName());
    assertTrue(a.service.outageLogin("Steve", "192.0.2.6").join().isEmpty()); // other address
    assertTrue(a.service.outageLogin("Alex", "192.0.2.5").join().isEmpty()); // never seen
    assertEquals(1, a.service.stats().outageLogins());
  }

  @Test
  void outageModeOffOrTooOld() throws Exception {
    FakeProxy off = outageProxy("a", false, 600);
    UUID steve = UUID.randomUUID();
    off.service.recordGoodLogin(steve, "Steve", "192.0.2.5");
    assertTrue(off.service.outageLogin("Steve", "192.0.2.5").join().isEmpty());
    off.service.setAuthOutageMode(true); // live toggle
    assertTrue(off.service.outageLogin("Steve", "192.0.2.5").join().isPresent());

    FakeProxy shortWindow = outageProxy("b", true, 1);
    UUID alex = UUID.randomUUID();
    shortWindow.service.recordGoodLogin(alex, "Alex", "192.0.2.5");
    Thread.sleep(1_100);
    assertTrue(shortWindow.service.outageLogin("Alex", "192.0.2.5").join().isEmpty());
  }

  @Test
  void epochIsAtLeastTheWallClock() {
    long before = System.currentTimeMillis();
    assertTrue(proxy("a").service.epoch() >= before);
  }

  @Test
  void serverNamesWithEqualsSignsKeepTheirCount() throws Exception {
    final FakeProxy a = proxy("a");
    final FakeProxy b = proxy("b");
    a.servers.put("odd=name", 4);
    await(() -> b.service.remotePlayerCount("odd=name") == 4);
    assertTrue(b.service.liveRemotes().get(0).servers().contains("odd=name"));
  }

  @Test
  void olderLoginsQuitDoesNotOverwriteNewerLastSeen() throws Exception {
    final FakeProxy a = proxy("a");
    UUID steve = UUID.randomUUID();
    String sep = String.valueOf((char) 9);
    long newer = System.currentTimeMillis() + 60_000;
    store.put("seen:" + steve, String.join(sep, "Steve", "z", "later", "5", Long.toString(newer)),
        86_400_000L).join();
    LoginResult r = a.login(steve, "Steve");
    a.service.serverChanged(steve, r.session(), "lobby");
    a.disconnectDuplicate(steve);
    Thread.sleep(300);
    var seen = a.service.lastSeen(steve).join().orElseThrow();
    assertEquals("z", seen.getProxyId());
    assertEquals(Optional.of("later"), seen.getServerName());
  }

  @Test
  void loginThatNeverReachedServerLeavesNoLastSeen() throws Exception {
    final FakeProxy a = proxy("a");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    a.disconnectDuplicate(steve); // aborted before any server
    Thread.sleep(300);
    assertEquals(Optional.empty(), a.service.lastSeen(steve).join());
    assertEquals(Optional.empty(), a.service.seenUuid("Steve").join());
  }
}
