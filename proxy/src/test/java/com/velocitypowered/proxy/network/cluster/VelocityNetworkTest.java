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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.network.NetworkPlayer;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.network.health.ProxyHealth;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The plugin-facing {@link VelocityNetwork} on mocked proxies sharing one in-memory store.
 */
class VelocityNetworkTest {

  @TempDir
  static Path keyDir;

  private final AtomicLong skew = new AtomicLong();
  private final LocalStore store = new LocalStore(() -> System.currentTimeMillis() + skew.get());
  private int lastSeenDays = 30;
  private final List<ClusterService> services = new ArrayList<>();

  /** One proxy of the test network. */
  private final class Node implements ClusterService.Host {
    final VelocityServer server = mock(VelocityServer.class);
    final Map<UUID, Long> sessions = new ConcurrentHashMap<>();
    final List<ConnectedPlayer> players = new ArrayList<>();
    final ClusterService cluster;
    final NetworkActions actions;
    final VelocityNetwork network;

    Node(String id, boolean enabled) {
      NetworkConfig config = new NetworkConfig(true, id, "", "", "local", "newest-wins",
          "stop-logins", "", 5, 120, "", 25565, 50,
          keyDir.resolve("network-secret.key").toString(), true, 21600, 300, false, false, 43200, false, 20, 60, lastSeenDays);
      cluster = new ClusterService(config, id, store, this);
      when(server.getAllPlayers()).thenAnswer(i -> List.copyOf(players));
      when(server.getHealth()).thenReturn(mock(ProxyHealth.class));
      when(server.getPlayerCount()).thenAnswer(i -> players.size());
      when(server.getServer(anyString())).thenAnswer(i -> {
        String name = i.getArgument(0);
        List<Player> here = players.stream().filter(p -> p.getConnectedServer().getServerInfo()
            .getName().equalsIgnoreCase(name)).map(p -> (Player) p).toList();
        if (here.isEmpty()) {
          return Optional.empty();
        }
        RegisteredServer rs = mock(RegisteredServer.class);
        when(rs.getPlayersConnected()).thenReturn(here);
        return Optional.of(rs);
      });
      actions = new NetworkActions(server, cluster);
      cluster.setActions(actions);
      cluster.start();
      services.add(cluster);
      network = new VelocityNetwork(server, enabled ? cluster : null, enabled ? actions : null);
    }

    @Override
    public int localPlayerCount() {
      return players.size();
    }

    @Override
    public Collection<String> localServers() {
      return localServerCounts().keySet();
    }

    @Override
    public Map<String, Integer> localServerCounts() {
      Map<String, Integer> counts = new HashMap<>();
      players.forEach(p -> counts.merge(p.getConnectedServer().getServerInfo().getName(), 1,
          Integer::sum));
      return counts;
    }

    @Override
    public void disconnectDuplicate(UUID player) {
    }

    @Override
    public void fire(Object event) {
    }

    /** Logs a player in and puts it on a server. */
    UUID join(String name, String serverName) throws Exception {
      UUID uuid = UUID.randomUUID();
      ClusterService.LoginResult r = cluster.claimLogin(uuid, name, "192.0.2.9", "[]")
          .get(10, TimeUnit.SECONDS);
      sessions.put(uuid, r.session());
      cluster.serverChanged(uuid, r.session(), serverName);
      ConnectedPlayer p = mock(ConnectedPlayer.class);
      VelocityServerConnection c = mock(VelocityServerConnection.class);
      when(c.getServerInfo()).thenReturn(new ServerInfo(serverName,
          new InetSocketAddress("127.0.0.1", 25566)));
      when(p.getConnectedServer()).thenReturn(c);
      when(p.getUniqueId()).thenReturn(uuid);
      when(p.getUsername()).thenReturn(name);
      when(p.getRemoteAddress()).thenReturn(new InetSocketAddress(
          InetAddress.getLoopbackAddress(), 40000));
      players.add(p);
      return uuid;
    }

    void quit(UUID uuid) {
      players.removeIf(p -> p.getUniqueId().equals(uuid));
      cluster.playerLeft(uuid, sessions.remove(uuid));
    }
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

  private static List<String> names(Collection<NetworkPlayer> players) {
    return players.stream().map(NetworkPlayer::getUsername).sorted().toList();
  }

  @Test
  void countsAndListsPerServerAndProxy() throws Exception {
    Node a = new Node("a", true);
    Node b = new Node("b", true);
    a.join("Alex", "lobby");
    a.join("Alice", "survival");
    b.join("Bob", "lobby");
    await(() -> a.network.getPlayerCount() == 3 && a.network.getPlayerCount("lobby") == 2
        && a.network.getPlayerCountOnProxy("b") == 1);
    assertEquals(1, a.network.getPlayerCount("survival"));
    assertEquals(0, a.network.getPlayerCount("creative"));
    assertEquals(2, a.network.getPlayerCountOnProxy("a"));
    assertEquals(0, a.network.getPlayerCountOnProxy("nope"));
    assertTrue(a.network.getProxy("b").isPresent());
    assertFalse(a.network.getProxy("nope").isPresent());

    assertEquals(List.of("Alex", "Bob"), names(a.network.getPlayers("lobby").get(5,
        TimeUnit.SECONDS)));
    assertEquals(List.of("Bob"), names(b.network.getPlayersOnProxy("b").get(5,
        TimeUnit.SECONDS)));
    assertEquals(List.of("Alex", "Alice"), names(b.network.getPlayersOnProxy("a").get(5,
        TimeUnit.SECONDS)));
    assertEquals(List.of(), names(b.network.getPlayersOnProxy("nope").get(5, TimeUnit.SECONDS)));
    assertEquals(List.of("Alice"), names(b.network.getPlayers("SURVIVAL").get(5,
        TimeUnit.SECONDS)));
  }

  @Test
  void onlineChecksAndNameLookups() throws Exception {
    Node a = new Node("a", true);
    Node b = new Node("b", true);
    UUID steve = b.join("Steve", "lobby");
    assertTrue(a.network.isOnline(steve).get(5, TimeUnit.SECONDS));
    assertTrue(a.network.isOnline("sTEVE").get(5, TimeUnit.SECONDS));
    assertFalse(a.network.isOnline(UUID.randomUUID()).get(5, TimeUnit.SECONDS));
    assertFalse(a.network.isOnline("nobody").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.of(steve), a.network.getUniqueId("steve").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.of("Steve"), a.network.getUsername(steve).get(5, TimeUnit.SECONDS));

    b.quit(steve);
    assertFalse(a.network.isOnline(steve).get(5, TimeUnit.SECONDS));
    // gone from the player list, still known from the shared profile cache
    assertEquals(Optional.of(steve), a.network.getUniqueId("STEVE").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.of("Steve"), a.network.getUsername(steve).get(5, TimeUnit.SECONDS));
    assertEquals(Optional.empty(), a.network.getUniqueId("nobody").get(5, TimeUnit.SECONDS));
  }

  @Test
  void disabledNetworkAnswersFromLocalState() throws Exception {
    Node a = new Node("a", false);
    assertEquals(0, a.network.getPlayerCount("lobby"));
    assertEquals(0, a.network.getPlayerCountOnProxy("x"));
    assertTrue(a.network.getPlayers("lobby").get(5, TimeUnit.SECONDS).isEmpty());
    assertTrue(a.network.getPlayersOnProxy("x").get(5, TimeUnit.SECONDS).isEmpty());
    assertFalse(a.network.isOnline(UUID.randomUUID()).get(5, TimeUnit.SECONDS));
    assertFalse(a.network.isOnline("x").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.empty(), a.network.getUniqueId("x").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.empty(), a.network.getUsername(UUID.randomUUID()).get(5,
        TimeUnit.SECONDS));
  }

  @Test
  void lastSeenIsKeptAfterQuitAndExpires() throws Exception {
    Node a = new Node("a", true);
    Node b = new Node("b", true);
    UUID steve = b.join("Steve", "lobby");
    var online = a.network.getLastSeen(steve).get(5, TimeUnit.SECONDS).orElseThrow();
    assertTrue(online.isOnline());
    assertEquals("b", online.getProxyId());
    assertEquals(Optional.of("lobby"), online.getServerName());

    b.quit(steve);
    long before = System.currentTimeMillis();
    var seen = a.network.getLastSeen(steve).get(5, TimeUnit.SECONDS).orElseThrow();
    assertFalse(seen.isOnline());
    assertEquals("Steve", seen.getUsername());
    assertEquals("b", seen.getProxyId());
    assertEquals(Optional.of("lobby"), seen.getServerName());
    assertTrue(Math.abs(seen.getTime().toEpochMilli() - before) < 5_000);
    assertEquals(Optional.of("lobby"), a.network.getLastServer(steve).get(5, TimeUnit.SECONDS));
    assertEquals(steve, a.network.getLastSeen("STEVE").get(5, TimeUnit.SECONDS).orElseThrow()
        .getUniqueId());

    // the profile cache forgets after its 6 hours, the last-seen record still knows the name
    skew.addAndGet(7 * 3_600_000L);
    assertEquals(Optional.of(steve), a.network.getUniqueId("steve").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.of("Steve"), a.network.getUsername(steve).get(5, TimeUnit.SECONDS));

    skew.addAndGet(31 * 86_400_000L);
    assertEquals(Optional.empty(), a.network.getLastSeen(steve).get(5, TimeUnit.SECONDS));
    assertEquals(Optional.empty(), a.network.getUniqueId("steve").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.empty(), a.network.getLastServer(steve).get(5, TimeUnit.SECONDS));
  }

  @Test
  void lastSeenOffWithZeroDays() throws Exception {
    lastSeenDays = 0;
    Node a = new Node("a", true);
    UUID steve = a.join("Steve", "lobby");
    a.quit(steve);
    assertEquals(Optional.empty(), a.network.getLastSeen(steve).get(5, TimeUnit.SECONDS));
  }

  @Test
  void lastSeenWithoutNetworkIsKeptInMemory() throws Exception {
    Node a = new Node("a", false);
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    when(configuration.getNetworkConfig()).thenReturn(NetworkConfig.DEFAULT);
    when(a.server.getConfiguration()).thenReturn(configuration);
    UUID steve = UUID.randomUUID();
    Player gone = mock(Player.class);
    when(gone.getUniqueId()).thenReturn(steve);
    when(gone.getUsername()).thenReturn("Steve");
    ServerConnection connection = mock(ServerConnection.class);
    when(connection.getServerInfo()).thenReturn(new ServerInfo("lobby",
        new InetSocketAddress("127.0.0.1", 25566)));
    when(gone.getCurrentServer()).thenReturn(Optional.of(connection));
    a.network.recordQuit(gone);
    var seen = a.network.getLastSeen(steve).get(5, TimeUnit.SECONDS).orElseThrow();
    assertEquals("local", seen.getProxyId());
    assertEquals(Optional.of("lobby"), seen.getServerName());
    assertEquals(Optional.of(steve), a.network.getUniqueId("STEVE").get(5, TimeUnit.SECONDS));
    assertEquals(Optional.of("Steve"), a.network.getUsername(steve).get(5, TimeUnit.SECONDS));
  }
}
