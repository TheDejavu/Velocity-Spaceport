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

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Sentinel failover drill, not a unit test. With SPACEPORT_DRILL_SENTINEL set to a
 * redis-sentinel:// URI it claims a player every 10 ms and publishes a sequence number every
 * 10 ms, runs SPACEPORT_DRILL_KILL (a shell command that kills the primary) after 5 s, and prints
 * how long claims failed, the pub/sub gap, and acknowledged claims missing after the failover.
 */
@EnabledIfEnvironmentVariable(named = "SPACEPORT_DRILL_SENTINEL", matches = ".+")
class RedisFailoverDrill {

  private record Attempt(long atMillis, UUID player, String error) {
  }

  @Test
  void drill() throws Exception {
    String uri = System.getenv("SPACEPORT_DRILL_SENTINEL");
    String kill = System.getenv("SPACEPORT_DRILL_KILL");
    int seconds = Integer.parseInt(System.getenv().getOrDefault("SPACEPORT_DRILL_SECONDS", "40"));
    String prefix = "spdrill:" + UUID.randomUUID() + ":";

    try (RedisStore store = new RedisStore(uri, prefix)) {
      store.tick("drill", 0, 600_000, "drill").join();
      long start = System.currentTimeMillis();
      AtomicLong lastSeq = new AtomicLong(-1);
      AtomicLong lost = new AtomicLong();
      List<long[]> gaps = new ArrayList<>(); // {at, gapMillis}
      AtomicLong lastRecv = new AtomicLong(start);
      store.subscribe("drill", payload -> {
        long seq = ByteBuffer.wrap(payload).getLong();
        long now = System.currentTimeMillis();
        long prev = lastSeq.getAndSet(seq);
        if (prev >= 0 && seq > prev + 1) {
          lost.addAndGet(seq - prev - 1);
        }
        long gap = now - lastRecv.getAndSet(now);
        if (gap > 100) {
          synchronized (gaps) {
            gaps.add(new long[] {now - start, gap});
          }
        }
      }).join();

      Thread publisher = Thread.ofPlatform().daemon().start(() -> {
        long seq = 0;
        while (!Thread.currentThread().isInterrupted()) {
          byte[] p = ByteBuffer.allocate(8).putLong(seq++).array();
          try {
            store.publish("drill", p).get(3, TimeUnit.SECONDS);
          } catch (Exception ignored) {
            // counted as a gap on the subscriber side
          }
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            return;
          }
        }
      });

      List<Attempt> attempts = new ArrayList<>();
      long killAt = -1;
      while (System.currentTimeMillis() - start < seconds * 1000L) {
        long now = System.currentTimeMillis();
        if (killAt < 0 && kill != null && now - start > 5000) {
          killAt = now - start;
          new ProcessBuilder("sh", "-c", kill).inheritIO().start();
        }
        UUID player = UUID.randomUUID();
        String error = null;
        try {
          store.claim(new NetworkStore.PlayerRecord(player, "p" + player.hashCode(), "drill", 1,
              null, "127.0.0.1", now), 1, 5000).get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
          Throwable c = e.getCause() != null ? e.getCause() : e;
          error = c.getClass().getSimpleName() + ": " + c.getMessage();
        }
        attempts.add(new Attempt(now - start, player, error));
        Thread.sleep(10);
      }
      publisher.interrupt();

      long firstErr = -1;
      long lastErr = -1;
      int errors = 0;
      Map<String, Integer> kinds = new TreeMap<>();
      for (Attempt a : attempts) {
        if (a.error() != null) {
          errors++;
          firstErr = firstErr < 0 ? a.atMillis() : firstErr;
          lastErr = a.atMillis();
          String kind = a.error().length() > 90 ? a.error().substring(0, 90) : a.error();
          kinds.merge(kind, 1, Integer::sum);
        }
      }
      // Acknowledged claims that the new primary no longer has (async replication loss).
      int missing = 0;
      long lastMissingAt = -1;
      try (RedisClient raw = RedisClient.create(uri);
           StatefulRedisConnection<String, String> conn = raw.connect()) {
        for (Attempt a : attempts) {
          if (a.error() == null
              && conn.sync().exists(prefix + "player:{" + a.player() + "}") == 0) {
            missing++;
            lastMissingAt = a.atMillis();
          }
        }
        for (String key : conn.sync().keys(prefix + "*")) {
          conn.sync().del(key);
        }
      }
      System.out.printf("[drill] attempts=%d killAt=%dms errors=%d firstErr=%dms lastErr=%dms "
              + "writeOutage=%dms missingAcked=%d lastMissingAt=%dms%n",
          attempts.size(), killAt, errors, firstErr, lastErr,
          errors == 0 ? 0 : lastErr - firstErr, missing, lastMissingAt);
      kinds.forEach((k, v) -> System.out.println("[drill]   " + v + " x " + k));
      System.out.println("[drill] pubsub lost=" + lost.get() + " gaps>100ms=" + gaps.stream()
          .map(g -> g[0] + "ms:" + g[1] + "ms").toList());
    }
  }
}
