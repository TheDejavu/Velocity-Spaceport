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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.proxy.network.cluster.MessageBus.Reply;
import java.io.DataInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Request/response over an in-memory store whose pub/sub can drop messages.
 */
class MessageBusTest {

  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
  private final LocalStore store = new LocalStore();
  /** Requests to drop before delivering again. */
  private final AtomicInteger drop = new AtomicInteger();
  /** Answers to drop before delivering again. */
  private final AtomicInteger dropAnswers = new AtomicInteger();

  private final NetworkStore lossy = (NetworkStore) java.lang.reflect.Proxy.newProxyInstance(
      getClass().getClassLoader(), new Class<?>[] {NetworkStore.class}, (proxy, method, args) -> {
        if (method.getName().equals("publish")
            && (((byte[]) args[1])[0] == MessageBus.RESPONSE ? dropAnswers : drop)
                .getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
          return CompletableFuture.completedFuture(null); // lost on the way
        }
        return method.invoke(store, args);
      });

  @AfterEach
  void stop() {
    timer.shutdownNow();
  }

  private MessageBus bus(String id) {
    MessageBus bus = new MessageBus(lossy, id, timer);
    store.subscribe("msg:" + id, payload -> {
      try {
        DataInputStream in = MessageBus.decode(payload);
        bus.onFrame(in.readByte(), in);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    return bus;
  }

  @Test
  void requestGetsAnswer() throws Exception {
    MessageBus a = bus("a");
    MessageBus b = bus("b");
    b.register((byte) 1, (from, in) -> CompletableFuture.completedFuture(
        new Reply(MessageBus.OK, (from + ":" + in.readUTF()).getBytes())));
    Reply reply = a.request("b", (byte) 1, MessageBus.encode(out -> out.writeUTF("hi")))
        .get(2, TimeUnit.SECONDS);
    assertTrue(reply.ok());
    assertArrayEquals("a:hi".getBytes(), reply.payload());
  }

  @Test
  void lostMessagesAreResentAndRunOnce() throws Exception {
    MessageBus a = bus("a");
    MessageBus b = bus("b");
    AtomicInteger runs = new AtomicInteger();
    b.register((byte) 1, (from, in) -> {
      runs.incrementAndGet();
      return CompletableFuture.completedFuture(Reply.of(MessageBus.OK));
    });
    dropAnswers.set(1); // the answer is lost, so the request is sent again
    assertTrue(a.request("b", (byte) 1, new byte[0]).get(5, TimeUnit.SECONDS).ok());
    assertEquals(1, runs.get());

    drop.set(3); // the next request and two resends are lost
    assertTrue(a.request("b", (byte) 1, new byte[0]).get(5, TimeUnit.SECONDS).ok());
    assertEquals(2, runs.get());
  }

  @Test
  void unknownActionAndMissingProxy() throws Exception {
    MessageBus a = bus("a");
    bus("b");
    assertEquals(MessageBus.UNKNOWN_ACTION,
        a.request("b", (byte) 9, new byte[0]).get(2, TimeUnit.SECONDS).status());
    ExecutionException e = assertThrows(ExecutionException.class,
        () -> a.request("nobody", (byte) 1, new byte[0]).get(10, TimeUnit.SECONDS));
    assertTrue(e.getCause() instanceof TimeoutException);
  }

  @Test
  void ownTimeoutIsExactAndForgetsTheRequest() throws Exception {
    MessageBus a = bus("a");
    long start = System.currentTimeMillis();
    ExecutionException e = assertThrows(ExecutionException.class,
        () -> a.request("nobody", (byte) 1, new byte[0], 400).get(5, TimeUnit.SECONDS));
    assertTrue(e.getCause() instanceof TimeoutException);
    long took = System.currentTimeMillis() - start;
    assertTrue(took >= 350 && took < 1_500, "took " + took);
    // a request the caller gave up on is not kept
    CompletableFuture<Reply> early = a.request("nobody", (byte) 1, new byte[0], 5_000);
    early.cancel(true);
    Thread.sleep(MessageBus.ATTEMPT_MILLIS + 300);
    assertEquals(0, a.pendingRequests());
  }
}
