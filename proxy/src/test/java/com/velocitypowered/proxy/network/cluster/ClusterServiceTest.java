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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.event.network.NetworkPlayerJoinEvent;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.network.cluster.ClusterService.LoginResult;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
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
    ClusterService service;

    @Override
    public int localPlayerCount() {
      return players.size();
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
      LoginResult r = service.claimLogin(uuid, name, "192.0.2.9").get(20, TimeUnit.SECONDS);
      if (r.denyReason() == null) {
        players.put(uuid, r.session());
      }
      return r;
    }
  }

  private FakeProxy proxy(String id, String duplicateLogin, String downPolicy) {
    FakeProxy p = new FakeProxy();
    NetworkConfig config = new NetworkConfig(true, id, "", "", "local", duplicateLogin,
        downPolicy);
    p.service = new ClusterService(config, id, store, p);
    p.service.start();
    services.add(p.service);
    return p;
  }

  private FakeProxy proxy(String id) {
    return proxy(id, "newest-wins", "stop-logins");
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
  void resyncKicksLocalCopyIfAnotherProxyTookThePlayer() throws Exception {
    FakeProxy a = proxy("a");
    FakeProxy b = proxy("b");
    UUID steve = UUID.randomUUID();
    a.login(steve, "Steve");
    store.setDown(true);
    store.wipe(); // the store came back empty...
    store.setDown(false);
    // ...and b got the player in before a noticed
    store.claim(new PlayerRecord(steve, "Steve", "b", 5, null, "192.0.2.4", 0), Long.MAX_VALUE,
        5_000).join();
    await(() -> a.players.isEmpty());
    assertEquals(Optional.of("b"), owner(steve));
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

  @Test
  void epochIsAtLeastTheWallClock() {
    long before = System.currentTimeMillis();
    assertTrue(proxy("a").service.epoch() >= before);
  }
}
