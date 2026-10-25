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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.event.network.NetworkPlayerJoinEvent;
import com.velocitypowered.api.event.network.NetworkPlayerQuitEvent;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import net.kyori.adventure.key.Key;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * The plugin-facing features (last seen, server counts, plugin channels, remote events) with two
 * proxies on a real Redis given by SPACEPORT_TEST_REDIS; skipped when it is not set. Every test
 * uses its own random key prefix.
 */
@EnabledIfEnvironmentVariable(named = "SPACEPORT_TEST_REDIS", matches = ".+")
class RedisNetworkApiTest {

  private static final String URI = System.getenv("SPACEPORT_TEST_REDIS");
  private static final Key CHANNEL = Key.key("test", "chat");

  @TempDir
  static Path keyDir;

  private final String prefix = "sptest:" + UUID.randomUUID() + ":";
  private final List<ClusterService> services = new ArrayList<>();
  private final Object plugin = new Object();

  /** One proxy of the test network. */
  private final class Node implements ClusterService.Host {
    final Map<UUID, Long> players = new ConcurrentHashMap<>();
    final Map<String, Integer> servers = new ConcurrentHashMap<>();
    final List<Object> events = new CopyOnWriteArrayList<>();
    final ClusterService service;
    final PluginMessenger messenger;

    Node(String id) {
      NetworkConfig config = new NetworkConfig(true, id, URI, prefix, "redis", "newest-wins",
          "stop-logins", "", 5, 120, "", 25565, 50,
          keyDir.resolve("network-secret.key").toString(), true, 21600, 300, false, false, 43200);
      service = new ClusterService(config, id, new RedisStore(URI, prefix), this);
      messenger = new PluginMessenger(null, service);
      service.start();
      services.add(service);
    }

    @Override
    public int localPlayerCount() {
      return players.size();
    }

    @Override
    public Collection<String> localServers() {
      return servers.keySet();
    }

    @Override
    public Map<String, Integer> localServerCounts() {
      return Map.copyOf(servers);
    }

    @Override
    public void disconnectDuplicate(UUID player) {
    }

    @Override
    public void fire(Object event) {
      events.add(event);
    }

    UUID join(String name, String server) throws Exception {
      UUID uuid = UUID.randomUUID();
      long session = service.claimLogin(uuid, name, "192.0.2.9", "[]").get(10, TimeUnit.SECONDS)
          .session();
      players.put(uuid, session);
      servers.merge(server, 1, Integer::sum);
      service.serverChanged(uuid, session, server);
      return uuid;
    }

    void quit(UUID uuid, String server) {
      servers.merge(server, -1, Integer::sum);
      servers.remove(server, 0);
      service.playerLeft(uuid, players.remove(uuid));
    }
  }

  @AfterEach
  void stop() {
    services.forEach(ClusterService::close);
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long end = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean()) {
      assertTrue(System.currentTimeMillis() < end, "timed out");
      Thread.sleep(20);
    }
  }

  @Test
  void lastSeenAndNamesSurviveInRedis() throws Exception {
    Node a = new Node("a");
    Node b = new Node("b");
    UUID steve = a.join("Steve", "lobby");
    a.quit(steve, "lobby");
    await(() -> b.service.lastSeen(steve).join().isPresent());
    var seen = b.service.lastSeen(steve).join().orElseThrow();
    assertEquals("Steve", seen.getUsername());
    assertEquals("a", seen.getProxyId());
    assertEquals(Optional.of("lobby"), seen.getServerName());
    assertEquals(Optional.of(steve), b.service.seenUuid("STEVE").join());
    assertEquals(Optional.empty(), b.service.lastSeen(UUID.randomUUID()).join());
  }

  @Test
  void serverCountsAndEventsReachTheOtherProxy() throws Exception {
    Node a = new Node("a");
    Node b = new Node("b");
    UUID steve = a.join("Steve", "lobby");
    a.join("Alex", "lobby");
    await(() -> b.service.remotePlayerCount("lobby") == 2);
    a.quit(steve, "lobby");
    await(() -> b.events.stream().anyMatch(e -> e instanceof NetworkPlayerQuitEvent));
    Thread.sleep(300);
    assertEquals(2, b.events.stream().filter(e -> e instanceof NetworkPlayerJoinEvent).count());
    assertEquals(1, b.events.stream().filter(e -> e instanceof NetworkPlayerQuitEvent).count());
  }

  @Test
  void pluginChannelsOverRedis() throws Exception {
    Node a = new Node("a");
    Node b = new Node("b");
    await(() -> a.service.liveRemotes().size() == 1 && b.service.liveRemotes().size() == 1);
    List<String> got = new CopyOnWriteArrayList<>();
    b.messenger.registerListener(plugin, CHANNEL, (c, from, p) ->
        got.add(from + ":" + new String(p, StandardCharsets.UTF_8)));
    b.messenger.registerRequestHandler(plugin, CHANNEL, (c, from, p) -> p);
    assertTrue(a.messenger.send(CHANNEL, "b", "hi".getBytes(StandardCharsets.UTF_8))
        .get(10, TimeUnit.SECONDS));
    assertEquals(1, a.messenger.broadcast(CHANNEL, "all".getBytes(StandardCharsets.UTF_8))
        .get(10, TimeUnit.SECONDS));
    await(() -> got.size() == 2);
    assertTrue(got.containsAll(List.of("a:hi", "a:all")));
    byte[] big = new byte[20_000];
    assertArrayEquals(big, a.messenger.request(CHANNEL, "b", big, Duration.ofSeconds(5))
        .get(10, TimeUnit.SECONDS));
  }
}
