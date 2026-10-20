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

import com.velocitypowered.proxy.network.cluster.MessageBus.Reply;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Message bus during a Sentinel failover, not a unit test. Proxy "a" sends a request to proxy "b"
 * every 50 ms while SPACEPORT_DRILL_KILL kills the Redis primary after 5 s; prints how many were
 * answered, how many timed out, how many ran twice, and the latencies.
 */
@EnabledIfEnvironmentVariable(named = "SPACEPORT_DRILL_SENTINEL", matches = ".+")
class BusFailoverDrill {

  private record Result(long sentAt, long millis, boolean ok) {
  }

  private static MessageBus bus(NetworkStore store, String id, ScheduledExecutorService timer) {
    MessageBus bus = new MessageBus(store, id, timer);
    store.subscribe("msg:" + id, payload -> {
      try {
        DataInputStream in = MessageBus.decode(payload);
        bus.onFrame(in.readByte(), in);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).join();
    return bus;
  }

  @Test
  void drill() throws Exception {
    String uri = System.getenv("SPACEPORT_DRILL_SENTINEL");
    String kill = System.getenv("SPACEPORT_DRILL_KILL");
    int seconds = Integer.parseInt(System.getenv().getOrDefault("SPACEPORT_DRILL_SECONDS", "25"));
    String prefix = "spbus:" + UUID.randomUUID() + ":";
    ScheduledExecutorService timer = Executors.newScheduledThreadPool(2);
    try (RedisStore storeA = new RedisStore(uri, prefix);
         RedisStore storeB = new RedisStore(uri, prefix)) {
      MessageBus a = bus(storeA, "a", timer);
      MessageBus b = bus(storeB, "b", timer);
      Map<Integer, AtomicInteger> runs = new ConcurrentHashMap<>();
      b.register((byte) 1, (from, in) -> {
        runs.computeIfAbsent(in.readInt(), k -> new AtomicInteger()).incrementAndGet();
        return CompletableFuture.completedFuture(Reply.of(MessageBus.OK));
      });
      List<Result> results = Collections.synchronizedList(new ArrayList<>());
      List<CompletableFuture<?>> all = new ArrayList<>();
      long start = System.currentTimeMillis();
      long killAt = -1;
      for (int i = 0; System.currentTimeMillis() - start < seconds * 1000L; i++) {
        long now = System.currentTimeMillis();
        if (killAt < 0 && kill != null && now - start > 5000) {
          killAt = now - start;
          new ProcessBuilder("sh", "-c", kill).inheritIO().start();
        }
        final int n = i;
        all.add(a.request("b", (byte) 1, MessageBus.encode(out -> out.writeInt(n)))
            .handle((r, ex) -> results.add(new Result(now - start,
                System.currentTimeMillis() - now, ex == null && r.ok()))));
        Thread.sleep(50);
      }
      CompletableFuture.allOf(all.toArray(CompletableFuture[]::new)).join();
      long ok = results.stream().filter(Result::ok).count();
      long twice = runs.values().stream().filter(c -> c.get() > 1).count();
      long[] lat = results.stream().filter(Result::ok).mapToLong(Result::millis).sorted()
          .toArray();
      long slowest = 0;
      long slowestAt = 0;
      for (Result r : results) {
        if (r.ok() && r.millis() > slowest) {
          slowest = r.millis();
          slowestAt = r.sentAt();
        }
      }
      System.out.printf("[bus] sent=%d answered=%d timedOut=%d ranTwice=%d killAt=%dms "
              + "latency p50=%dms p99=%dms max=%dms (sent at %dms)%n",
          results.size(), ok, results.size() - ok, twice, killAt, lat[lat.length / 2],
          lat[(int) (lat.length * 0.99)], slowest, slowestAt);
      results.stream().filter(r -> !r.ok()).limit(3)
          .forEach(r -> System.out.println("[bus]   timed out: sent at " + r.sentAt() + "ms"));
    } finally {
      timer.shutdownNow();
    }
  }
}
