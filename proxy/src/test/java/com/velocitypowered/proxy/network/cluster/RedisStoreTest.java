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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.proxy.network.cluster.NetworkStore.ClaimResult;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Runs against a real Redis given by SPACEPORT_TEST_REDIS (a redis:// or redis-sentinel:// URI);
 * skipped when it is not set. Uses its own random key prefix.
 */
@EnabledIfEnvironmentVariable(named = "SPACEPORT_TEST_REDIS", matches = ".+")
class RedisStoreTest {

  private static final String URI = System.getenv("SPACEPORT_TEST_REDIS");
  private static final String PREFIX = "sptest:" + UUID.randomUUID() + ":";
  private static RedisStore store;
  private static RedisClient raw;
  private static StatefulRedisConnection<String, String> rawConn;
  private static RedisCommands<String, String> redis;

  @BeforeAll
  static void connect() {
    store = new RedisStore(URI, PREFIX);
    raw = RedisClient.create(URI);
    rawConn = raw.connect();
    redis = rawConn.sync();
  }

  @AfterAll
  static void close() {
    for (String key : redis.keys(PREFIX + "*")) {
      redis.del(key);
    }
    rawConn.close();
    raw.shutdown();
    store.close();
  }

  @Test
  void heartbeatExpires() {
    store.heartbeat("hb", 1000).join();
    long ttl = redis.pttl(PREFIX + "proxy:hb");
    assertTrue(ttl > 0 && ttl <= 1000, "ttl " + ttl);
  }

  @Test
  void claimRespectsLiveOwnerAndTakesFromDeadOne() {
    UUID player = UUID.randomUUID();
    store.heartbeat("a", 60_000).join();
    store.heartbeat("b", 60_000).join();

    assertEquals(new ClaimResult(true, null), store.claim(player, "a", 1).join());
    assertEquals(new ClaimResult(false, "a"), store.claim(player, "b", 1).join());
    assertEquals(new ClaimResult(true, "a"), store.claim(player, "a", 1).join());

    redis.del(PREFIX + "proxy:a"); // a died
    assertEquals(new ClaimResult(true, "a"), store.claim(player, "b", 1).join());
    assertEquals("b", redis.hget(PREFIX + "player:{" + player + "}", "proxy"));
  }

  @Test
  void staleEpochCannotOverwriteOrRelease() {
    UUID player = UUID.randomUUID();
    store.heartbeat("c", 60_000).join();
    assertTrue(store.claim(player, "c", 5).join().claimed());
    assertFalse(store.claim(player, "c", 3).join().claimed());
    assertFalse(store.release(player, "c", 3).join());
    assertFalse(store.release(player, "d", 5).join());
    assertTrue(store.release(player, "c", 5).join());
    assertNull(redis.hget(PREFIX + "player:{" + player + "}", "proxy"));
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

  /** Not an assertion: prints round-trip latency for the decision note. */
  @Test
  void latency() throws Exception {
    int n = Integer.getInteger("spaceport.test.redis.n", 2000);
    store.heartbeat("lat", 600_000).join();
    long[] claim = new long[n];
    for (int i = 0; i < n; i++) {
      UUID player = UUID.randomUUID();
      long t = System.nanoTime();
      store.claim(player, "lat", 1).join();
      claim[i] = System.nanoTime() - t;
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
    System.out.println("[lettuce] claim " + summary(claim) + " | pub/sub " + summary(pubsub));
  }

  static String summary(long[] nanos) {
    long[] s = nanos.clone();
    Arrays.sort(s);
    return String.format("n=%d p50=%.3fms p99=%.3fms max=%.3fms", s.length,
        s[s.length / 2] / 1e6, s[(int) (s.length * 0.99)] / 1e6, s[s.length - 1] / 1e6);
  }
}
