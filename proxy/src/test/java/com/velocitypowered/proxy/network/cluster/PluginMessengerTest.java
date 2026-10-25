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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.NetworkMessenger;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import net.kyori.adventure.key.Key;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Plugin channels between proxies sharing one in-memory store.
 */
class PluginMessengerTest {

  private static final Key CHANNEL = Key.key("test", "chat");

  @TempDir
  static Path keyDir;

  private final LocalStore store = new LocalStore();
  private final List<ClusterService> services = new ArrayList<>();
  private final List<PluginMessenger> messengers = new ArrayList<>();
  private final Object plugin = new Object();

  private PluginMessenger proxy(String id) {
    NetworkConfig config = new NetworkConfig(true, id, "", "", "local", "newest-wins",
        "stop-logins", "", 5, 120, "", 25565, 50,
        keyDir.resolve("network-secret.key").toString(), true, 21600, 300, false, false, 43200);
    ClusterService service = new ClusterService(config, id, store, new ClusterService.Host() {
      @Override
      public int localPlayerCount() {
        return 0;
      }

      @Override
      public Collection<String> localServers() {
        return List.of();
      }

      @Override
      public void disconnectDuplicate(UUID player) {
      }

      @Override
      public void fire(Object event) {
      }
    });
    PluginMessenger messenger = new PluginMessenger(null, service);
    service.start();
    services.add(service);
    messengers.add(messenger);
    return messenger;
  }

  @AfterEach
  void stop() {
    messengers.forEach(PluginMessenger::close);
    services.forEach(ClusterService::close);
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long end = System.currentTimeMillis() + 5_000;
    while (!condition.getAsBoolean()) {
      assertTrue(System.currentTimeMillis() < end, "timed out");
      Thread.sleep(20);
    }
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  /** Waits until the proxies see each other, as plugins would after startup. */
  private void meet(int proxies) throws Exception {
    await(() -> services.stream().allMatch(s -> s.liveRemotes().size() == proxies - 1));
  }

  @Test
  void sendReachesListenerOnceOffTheBusThread() throws Exception {
    PluginMessenger a = proxy("a");
    PluginMessenger b = proxy("b");
    meet(2);
    List<String> got = new CopyOnWriteArrayList<>();
    List<String> threads = new CopyOnWriteArrayList<>();
    b.registerListener(plugin, CHANNEL, (channel, from, payload) -> {
      got.add(channel.asString() + " " + from + " " + new String(payload, StandardCharsets.UTF_8));
      threads.add(Thread.currentThread().getName());
    });
    assertTrue(a.send(CHANNEL, "b", bytes("hi")).get(5, TimeUnit.SECONDS));
    await(() -> got.size() == 1);
    Thread.sleep(300);
    assertEquals(List.of("test:chat a hi"), got);
    assertTrue(threads.get(0).startsWith("Spaceport Plugin Messages"), threads.get(0));
  }

  @Test
  void slowListenerDoesNotHoldBackTheAcknowledgement() throws Exception {
    PluginMessenger a = proxy("a");
    PluginMessenger b = proxy("b");
    meet(2);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch entered = new CountDownLatch(1);
    b.registerListener(plugin, CHANNEL, (channel, from, payload) -> {
      entered.countDown();
      try {
        release.await();
      } catch (InterruptedException ignored) {
        // test over
      }
    });
    assertTrue(a.send(CHANNEL, "b", bytes("x")).get(2, TimeUnit.SECONDS));
    assertTrue(entered.await(2, TimeUnit.SECONDS));
    // the bus still answers while the listener is stuck
    b.registerRequestHandler(plugin, Key.key("test", "ping"), (c, f, p) -> bytes("pong"));
    assertArrayEquals(bytes("pong"), a.request(Key.key("test", "ping"), "b", new byte[0],
        Duration.ofSeconds(2)).get(3, TimeUnit.SECONDS));
    release.countDown();
  }

  @Test
  void sendWithoutListenerOrToUnknownProxyIsFalse() throws Exception {
    PluginMessenger a = proxy("a");
    proxy("b");
    meet(2);
    assertFalse(a.send(CHANNEL, "b", bytes("x")).get(5, TimeUnit.SECONDS));
    assertFalse(a.send(CHANNEL, "nope", bytes("x")).get(5, TimeUnit.SECONDS));
  }

  @Test
  void broadcastGoesToOtherProxiesOnly() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    final PluginMessenger c = proxy("c");
    proxy("d"); // no listener
    meet(4);
    final AtomicInteger atA = new AtomicInteger();
    final AtomicInteger atB = new AtomicInteger();
    final AtomicInteger atC = new AtomicInteger();
    a.registerListener(plugin, CHANNEL, (ch, from, p) -> atA.incrementAndGet());
    b.registerListener(plugin, CHANNEL, (ch, from, p) -> atB.incrementAndGet());
    c.registerListener(plugin, CHANNEL, (ch, from, p) -> atC.incrementAndGet());
    assertEquals(2, a.broadcast(CHANNEL, bytes("all")).get(5, TimeUnit.SECONDS));
    await(() -> atB.get() == 1 && atC.get() == 1);
    Thread.sleep(300);
    assertEquals(0, atA.get());
    assertEquals(1, atB.get());
    assertEquals(1, atC.get());
  }

  @Test
  void requestGetsTheHandlersAnswer() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    meet(2);
    b.registerRequestHandler(plugin, CHANNEL, (channel, from, payload) ->
        bytes(from + ":" + new String(payload, StandardCharsets.UTF_8).toUpperCase()));
    assertArrayEquals(bytes("a:PING"), a.request(CHANNEL, "b", bytes("ping"),
        Duration.ofSeconds(3)).get(5, TimeUnit.SECONDS));
  }

  @Test
  void requestFailuresAreDistinct() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    meet(2);
    Key slow = Key.key("test", "slow");
    Key broken = Key.key("test", "broken");
    b.registerRequestHandler(plugin, slow, (c, f, p) -> {
      Thread.sleep(1_500);
      return p;
    });
    b.registerRequestHandler(plugin, broken, (c, f, p) -> {
      throw new IllegalStateException("boom");
    });

    ExecutionException timeout = assertThrows(ExecutionException.class,
        () -> a.request(slow, "b", bytes("x"), Duration.ofMillis(300)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(TimeoutException.class, timeout.getCause());

    ExecutionException none = assertThrows(ExecutionException.class,
        () -> a.request(CHANNEL, "b", bytes("x"), Duration.ofSeconds(2)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, none.getCause());

    ExecutionException failed = assertThrows(ExecutionException.class,
        () -> a.request(broken, "b", bytes("x"), Duration.ofSeconds(2)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, failed.getCause());

    ExecutionException unknown = assertThrows(ExecutionException.class,
        () -> a.request(slow, "nope", bytes("x"), Duration.ofSeconds(2)).get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalArgumentException.class, unknown.getCause());
  }

  @Test
  void limitsAreEnforced() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    meet(2);
    byte[] big = new byte[NetworkMessenger.MAX_PAYLOAD_BYTES + 1];
    assertThrows(IllegalArgumentException.class, () -> a.send(CHANNEL, "b", big));
    assertThrows(IllegalArgumentException.class, () -> a.broadcast(CHANNEL, big));
    assertThrows(IllegalArgumentException.class,
        () -> a.request(CHANNEL, "b", big, Duration.ofSeconds(1)));
    assertThrows(IllegalArgumentException.class,
        () -> a.request(CHANNEL, "b", new byte[0], Duration.ZERO));
    // the largest payload still goes through, and a too large answer fails the request
    byte[] max = new byte[NetworkMessenger.MAX_PAYLOAD_BYTES];
    b.registerRequestHandler(plugin, CHANNEL, (c, f, p) -> p.length == 0 ? big : p);
    assertEquals(max.length, a.request(CHANNEL, "b", max, Duration.ofSeconds(3))
        .get(5, TimeUnit.SECONDS).length);
    ExecutionException tooBig = assertThrows(ExecutionException.class,
        () -> a.request(CHANNEL, "b", new byte[0], Duration.ofSeconds(3)).get(5,
            TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, tooBig.getCause());
  }

  @Test
  void unregisteringStopsDeliveryAndSurvivesFailingListeners() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    meet(2);
    Object other = new Object();
    AtomicInteger mine = new AtomicInteger();
    AtomicInteger theirs = new AtomicInteger();
    b.registerListener(plugin, CHANNEL, (ch, from, p) -> {
      mine.incrementAndGet();
      throw new IllegalStateException("a failing plugin");
    });
    b.registerListener(other, CHANNEL, (ch, from, p) -> theirs.incrementAndGet());
    Key second = Key.key("test", "second");
    b.registerListener(plugin, second, (ch, from, p) -> mine.incrementAndGet());
    b.registerRequestHandler(plugin, CHANNEL, (c, f, p) -> p);
    assertThrows(IllegalStateException.class,
        () -> b.registerRequestHandler(other, CHANNEL, (c, f, p) -> p));

    assertTrue(a.send(CHANNEL, "b", bytes("1")).get(5, TimeUnit.SECONDS));
    await(() -> mine.get() == 1 && theirs.get() == 1);

    b.unregisterAll(plugin); // what a plugin does when it stops
    assertTrue(a.send(CHANNEL, "b", bytes("2")).get(5, TimeUnit.SECONDS)); // other's listener
    assertFalse(a.send(second, "b", bytes("2")).get(5, TimeUnit.SECONDS));
    await(() -> theirs.get() == 2);
    assertEquals(1, mine.get());
    assertThrows(ExecutionException.class, () -> a.request(CHANNEL, "b", bytes("x"),
        Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS));

    b.unregister(other, CHANNEL);
    assertFalse(a.send(CHANNEL, "b", bytes("3")).get(5, TimeUnit.SECONDS));
  }

  @Test
  void worksOnThisProxyWithoutNetwork() throws Exception {
    PluginMessenger alone = new PluginMessenger(null, null);
    AtomicInteger got = new AtomicInteger();
    alone.registerListener(plugin, CHANNEL, (ch, from, p) -> got.incrementAndGet());
    alone.registerRequestHandler(plugin, Key.key("test", "ping"), (c, f, p) -> bytes("pong"));
    assertTrue(alone.send(CHANNEL, "local", bytes("x")).get(5, TimeUnit.SECONDS));
    await(() -> got.get() == 1);
    assertFalse(alone.send(CHANNEL, "other", bytes("x")).get(5, TimeUnit.SECONDS));
    assertEquals(0, alone.broadcast(CHANNEL, bytes("x")).get(5, TimeUnit.SECONDS));
    assertArrayEquals(bytes("pong"), alone.request(Key.key("test", "ping"), "local",
        new byte[0], Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS));
    ExecutionException unknown = assertThrows(ExecutionException.class,
        () -> alone.request(CHANNEL, "other", new byte[0], Duration.ofSeconds(1))
            .get(5, TimeUnit.SECONDS));
    assertInstanceOf(IllegalArgumentException.class, unknown.getCause());
    alone.close();
  }

  @Test
  void slowChannelCannotFillTheQueueForOthers() throws Exception {
    PluginMessenger alone = new PluginMessenger(null, null);
    CountDownLatch release = new CountDownLatch(1);
    Key slow = Key.key("test", "slow");
    alone.registerListener(plugin, slow, (ch, from, p) -> {
      try {
        release.await();
      } catch (InterruptedException ignored) {
        // test over
      }
    });
    AtomicInteger got = new AtomicInteger();
    alone.registerListener(plugin, CHANNEL, (ch, from, p) -> got.incrementAndGet());
    int accepted = 0;
    for (int i = 0; i < PluginMessenger.CHANNEL_IN_FLIGHT_MAX + 50; i++) {
      if (alone.send(slow, "local", bytes("x")).get(2, TimeUnit.SECONDS)) {
        accepted++;
      }
    }
    assertEquals(PluginMessenger.CHANNEL_IN_FLIGHT_MAX, accepted);
    // the slow channel is full, but its pool threads are stuck: the other channel still queues
    assertTrue(alone.send(CHANNEL, "local", bytes("y")).get(2, TimeUnit.SECONDS));
    release.countDown();
    await(() -> got.get() == 1);
    alone.close();
  }

  @Test
  void listenersGetTheirOwnCopyOfThePayload() throws Exception {
    PluginMessenger alone = new PluginMessenger(null, null);
    byte[] sent = bytes("abc");
    CountDownLatch seen = new CountDownLatch(2);
    List<String> got = new CopyOnWriteArrayList<>();
    for (int i = 0; i < 2; i++) {
      alone.registerListener(new Object(), CHANNEL, (ch, from, p) -> {
        got.add(new String(p, StandardCharsets.UTF_8));
        p[0] = 'X'; // must not leak to the other listener or the sender
        seen.countDown();
      });
    }
    alone.send(CHANNEL, "local", sent).get(2, TimeUnit.SECONDS);
    assertTrue(seen.await(2, TimeUnit.SECONDS));
    assertEquals(List.of("abc", "abc"), got);
    assertArrayEquals(bytes("abc"), sent);
    alone.close();
  }

  @Test
  void failuresAreTheSameTypeLocallyAndRemotely() throws Exception {
    final PluginMessenger a = proxy("a");
    final PluginMessenger b = proxy("b");
    meet(2);
    Key slow = Key.key("test", "slow");
    b.registerRequestHandler(plugin, slow, (c, f, p) -> {
      Thread.sleep(1_500);
      return p;
    });
    a.registerRequestHandler(plugin, slow, (c, f, p) -> {
      Thread.sleep(1_500);
      return p;
    });
    // seen from a callback, no CompletionException wraps any of them
    assertEquals(TimeoutException.class, a.request(slow, "b", bytes("x"), Duration.ofMillis(300))
        .handle((v, ex) -> ex.getClass()).get(5, TimeUnit.SECONDS));
    assertEquals(TimeoutException.class, a.request(slow, "a", bytes("x"), Duration.ofMillis(300))
        .handle((v, ex) -> ex.getClass()).get(5, TimeUnit.SECONDS));
    assertEquals(IllegalStateException.class, a.request(CHANNEL, "b", bytes("x"),
        Duration.ofSeconds(2)).handle((v, ex) -> ex.getClass()).get(5, TimeUnit.SECONDS));
    assertEquals(IllegalStateException.class, a.request(CHANNEL, "a", bytes("x"),
        Duration.ofSeconds(2)).handle((v, ex) -> ex.getClass()).get(5, TimeUnit.SECONDS));
    assertEquals(IllegalArgumentException.class, a.request(CHANNEL, "nope", bytes("x"),
        Duration.ofSeconds(2)).handle((v, ex) -> ex.getClass()).get(5, TimeUnit.SECONDS));
  }

  @Test
  void registeringWhileUnregisteringLosesNothing() throws Exception {
    PluginMessenger alone = new PluginMessenger(null, null);
    Object other = new Object();
    alone.registerListener(other, CHANNEL, (ch, from, p) -> { });
    AtomicInteger got = new AtomicInteger();
    int rounds = 200;
    Thread unregistering = new Thread(() -> {
      for (int i = 0; i < rounds; i++) {
        alone.unregister(other, CHANNEL);
        alone.registerListener(other, CHANNEL, (ch, from, p) -> { });
      }
    });
    Object mine = new Object();
    unregistering.start();
    for (int i = 0; i < rounds; i++) {
      alone.registerListener(mine, CHANNEL, (ch, from, p) -> got.incrementAndGet());
    }
    unregistering.join();
    // every registration of mine survived the concurrent unregistering of the other plugin
    assertTrue(alone.send(CHANNEL, "local", bytes("x")).get(2, TimeUnit.SECONDS));
    await(() -> got.get() == rounds);
    alone.close();
  }
}
