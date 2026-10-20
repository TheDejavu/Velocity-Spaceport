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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.proxy.network.cluster.NetworkStore.Claim;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import com.velocitypowered.proxy.network.cluster.NetworkStore.Tick;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs against a real Redis given by SPACEPORT_TEST_REDIS (a redis:// or redis-sentinel:// URI);
 * skipped when it is not set. Every test uses its own random key prefix.
 */
@EnabledIfEnvironmentVariable(named = "SPACEPORT_TEST_REDIS", matches = ".+")
class RedisStoreTest {

  private static final String URI = System.getenv("SPACEPORT_TEST_REDIS");
  private String prefix;
  private RedisStore store;
  private RedisClient raw;
  private StatefulRedisConnection<String, String> rawConn;
  private RedisCommands<String, String> redis;

  @BeforeEach
  void connect() {
    prefix = "sptest:" + UUID.randomUUID() + ":";
    store = new RedisStore(URI, prefix);
    raw = RedisClient.create(URI);
    rawConn = raw.connect();
    redis = rawConn.sync();
  }

  @AfterEach
  void close() {
    for (String key : redis.keys(prefix + "*")) {
      redis.del(key);
    }
    rawConn.close();
    raw.shutdown();
    store.close();
  }

  private static PlayerRecord record(UUID uuid, String name, String proxy, long session) {
    return new PlayerRecord(uuid, name, proxy, session, null, "192.0.2.3", 1000);
  }

  @Test
  void tickTracksProxiesAndGeneration() {
    Tick first = store.tick("a", 3, 15_000, "gen-a").join();
    assertEquals("gen-a", first.generation());
    assertTrue(first.generationCreated());
    assertFalse(first.selfExisted());

    Tick second = store.tick("b", 5, 15_000, "gen-b").join();
    assertEquals("gen-a", second.generation());
    assertFalse(second.generationCreated());
    assertEquals(2, second.proxies().size());
    ProxyState a = second.proxies().stream().filter(p -> p.id().equals("a")).findFirst()
        .orElseThrow();
    assertEquals(3, a.players());
    assertTrue(a.ageMillis() >= 0 && a.ageMillis() < 5_000);
    assertTrue(store.tick("a", 4, 15_000, "x").join().selfExisted());

    long ttl = redis.pttl(prefix + "{net}:proxy:a");
    assertTrue(ttl > 0 && ttl <= 15_000, "ttl " + ttl);

    redis.del(prefix + "{net}:gen"); // data loss
    Tick after = store.tick("a", 4, 15_000, "gen-new").join();
    assertEquals("gen-new", after.generation());
    assertTrue(after.generationCreated());
  }

  @Test
  void claimRespectsLiveOwnerAndTakesFromSilentOne() throws Exception {
    UUID player = UUID.randomUUID();
    store.tick("a", 0, 15_000, "g").join();
    store.tick("b", 0, 15_000, "g").join();

    assertEquals(new Claim(true, null), store.claim(record(player, "Steve", "a", 1), 1, 5_000)
        .join());
    assertEquals(new Claim(false, "a"), store.claim(record(player, "Steve", "b", 2), 1, 5_000)
        .join());

    Thread.sleep(30); // "a" has now been silent for 30 ms
    assertEquals(new Claim(true, "a"), store.claim(record(player, "Steve", "b", 2), 1, 20)
        .join());
    assertEquals("b", redis.hget(prefix + "player:{" + player + "}", "proxy"));
    assertFalse(redis.sismember(prefix + "{net}:members:a", player.toString()));
    assertTrue(redis.sismember(prefix + "{net}:members:b", player.toString()));
  }

  @Test
  void ownerWithoutRecordIsDead() {
    UUID player = UUID.randomUUID();
    store.claim(record(player, "Alex", "gone", 1), 1, 5_000).join();
    assertTrue(store.claim(record(player, "Alex", "b", 2), 1, 5_000).join().claimed());
  }

  @Test
  void staleEpochCannotOverwrite() {
    UUID player = UUID.randomUUID();
    assertTrue(store.claim(record(player, "Alex", "c", 1), 5, 5_000).join().claimed());
    assertFalse(store.claim(record(player, "Alex", "c", 2), 3, 5_000).join().claimed());
    assertTrue(store.claim(record(player, "Alex", "c", 3), 5, 5_000).join().claimed());
  }

  @Test
  void sessionGuardsUpdateAndRelease() {
    UUID player = UUID.randomUUID();
    store.claim(record(player, "Alex", "c", 7), 1, 5_000).join();
    assertFalse(store.updateServer(player, "c", 8, "lobby").join());
    assertTrue(store.updateServer(player, "c", 7, "lobby").join());
    assertEquals(Optional.of("lobby"), store.player(player).join().map(PlayerRecord::server));
    assertEquals(Optional.of(player), store.uuidOf("ALEX").join());

    assertFalse(store.release(player, "c", 8).join());
    assertFalse(store.release(player, "d", 7).join());
    assertTrue(store.release(player, "c", 7).join());
    assertEquals(Optional.empty(), store.player(player).join());
    assertEquals(Optional.empty(), store.uuidOf("alex").join());
  }

  @Test
  void sweepRemovesOnlyDeadProxies() {
    UUID one = UUID.randomUUID();
    UUID two = UUID.randomUUID();
    store.tick("d", 2, 15_000, "g").join();
    store.claim(record(one, "One", "d", 1), 1, 5_000).join();
    store.claim(record(two, "Two", "d", 2), 1, 5_000).join();

    assertEquals(-1, store.sweep("d", 5_000, false, 0).join());
    assertEquals(2, store.sweep("d", 5_000, true, 0).join());
    assertEquals(Optional.empty(), store.player(one).join());
    assertEquals(Optional.empty(), store.uuidOf("two").join());
    assertFalse(redis.sismember(prefix + "{net}:proxies", "d"));
    assertFalse(store.tick("e", 0, 15_000, "g").join().proxies().stream()
        .anyMatch(p -> p.id().equals("d")));
  }

  @Test
  void epochsIncrease() {
    long first = store.nextEpoch().join();
    assertTrue(store.nextEpoch().join() > first);
  }

  @Test
  void publishSubscribeRoundTrip() throws Exception {
    BlockingQueue<byte[]> received = new ArrayBlockingQueue<>(16);
    store.subscribe("msg:all", received::add).join();
    byte[] payload = {1, 2, 3, (byte) 0xff};
    store.publish("msg:all", payload).join();
    assertArrayEquals(payload, received.poll(2, TimeUnit.SECONDS));
  }

  /** Not an assertion: prints round-trip latency. */
  @Test
  void latency() throws Exception {
    int n = 2000;
    store.tick("lat", 0, 600_000, "g").join();
    long[] claim = new long[n];
    for (int i = 0; i < n; i++) {
      long t = System.nanoTime();
      store.claim(record(UUID.randomUUID(), "p" + i, "lat", i), 1, 5_000).join();
      claim[i] = System.nanoTime() - t;
    }
    long[] tick = new long[n / 10];
    for (int i = 0; i < tick.length; i++) {
      long t = System.nanoTime();
      store.tick("lat", i, 15_000, "g").join();
      tick[i] = System.nanoTime() - t;
    }
    BlockingQueue<byte[]> received = new ArrayBlockingQueue<>(4);
    store.subscribe("msg:lat", received::add).join();
    long[] pubsub = new long[n];
    byte[] payload = "ping".getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < n; i++) {
      long t = System.nanoTime();
      store.publish("msg:lat", payload);
      received.take();
      pubsub[i] = System.nanoTime() - t;
    }
    System.out.println("[lettuce] claim " + summary(claim) + " | tick " + summary(tick)
        + " | pub/sub " + summary(pubsub));
  }

  static String summary(long[] nanos) {
    long[] s = nanos.clone();
    Arrays.sort(s);
    return String.format("n=%d p50=%.3fms p99=%.3fms max=%.3fms", s.length,
        s[s.length / 2] / 1e6, s[(int) (s.length * 0.99)] / 1e6, s[s.length - 1] / 1e6);
  }
}
