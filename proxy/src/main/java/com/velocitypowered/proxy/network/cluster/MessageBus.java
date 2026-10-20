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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Request/response between proxies over the store's pub/sub. Pub/sub drops messages (for example
 * during a Redis failover), so a request is resent until it is answered or runs out of attempts,
 * and the receiver remembers request ids for a while so a resent request runs only once.
 */
public final class MessageBus {

  /** Frame types, next to the ones in {@link ClusterService}. */
  static final byte REQUEST = 10;
  static final byte RESPONSE = 11;

  public static final byte OK = 0;
  /** The player is not on the asked proxy (any more). */
  public static final byte NOT_HERE = 1;
  public static final byte FAILED = 2;
  public static final byte UNKNOWN_ACTION = 3;

  static final long ATTEMPT_MILLIS = 750;
  /** 8 attempts = 6 s: a Sentinel failover (3 s) plus the pub/sub connections coming back. */
  static final int ATTEMPTS = 8;

  private static final Logger logger = LogManager.getLogger(MessageBus.class);

  /**
   * An answer.
   *
   * @param status OK, NOT_HERE, FAILED or UNKNOWN_ACTION
   * @param payload the answer's data
   */
  public record Reply(byte status, byte[] payload) {
    public boolean ok() {
      return status == OK;
    }

    static Reply of(byte status) {
      return new Reply(status, new byte[0]);
    }
  }

  /** Handles one kind of request on the receiving proxy. Must not block. */
  @FunctionalInterface
  public interface Handler {
    CompletableFuture<Reply> handle(String from, DataInputStream in) throws IOException;
  }

  /** Writes a payload. */
  @FunctionalInterface
  public interface Writer {
    void write(DataOutputStream out) throws IOException;
  }

  private final NetworkStore store;
  private final String proxyId;
  private final ScheduledExecutorService timer;
  private final Map<Byte, Handler> handlers = new ConcurrentHashMap<>();
  private final Map<Long, CompletableFuture<Reply>> pending = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Reply>> handled = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofSeconds(30))
      .build();
  private final AtomicLong ids = new AtomicLong(ThreadLocalRandom.current().nextLong());

  MessageBus(NetworkStore store, String proxyId, ScheduledExecutorService timer) {
    this.store = store;
    this.proxyId = proxyId;
    this.timer = timer;
  }

  public void register(byte action, Handler handler) {
    handlers.put(action, handler);
  }

  /**
   * Encodes a payload.
   *
   * @param writer writes the fields
   * @return the bytes
   */
  public static byte[] encode(Writer writer) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(64);
      writer.write(new DataOutputStream(bytes));
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  public static DataInputStream decode(byte[] payload) {
    return new DataInputStream(new ByteArrayInputStream(payload));
  }

  /**
   * Sends a request to a proxy and waits for its answer, resending it up to {@link #ATTEMPTS}
   * times. Fails with a {@link TimeoutException} if no answer came.
   *
   * @param proxy the proxy to ask
   * @param action what to do
   * @param payload the request's data
   * @return the answer
   */
  public CompletableFuture<Reply> request(String proxy, byte action, byte[] payload) {
    long id = ids.incrementAndGet();
    CompletableFuture<Reply> reply = new CompletableFuture<>();
    pending.put(id, reply);
    byte[] frame = encode(out -> {
      out.writeByte(REQUEST);
      out.writeUTF(proxyId);
      out.writeLong(id);
      out.writeByte(action);
      out.writeInt(payload.length);
      out.write(payload);
    });
    send(proxy, id, frame, reply, 1);
    return reply;
  }

  private void send(String proxy, long id, byte[] frame, CompletableFuture<Reply> reply,
      int attempt) {
    store.publish("msg:" + proxy, frame).exceptionally(ex -> null); // a failed send just times out
    timer.schedule(() -> {
      if (reply.isDone()) {
        return;
      }
      if (attempt < ATTEMPTS) {
        send(proxy, id, frame, reply, attempt + 1);
      } else {
        pending.remove(id);
        reply.completeExceptionally(new TimeoutException("no answer from proxy " + proxy));
      }
    }, ATTEMPT_MILLIS, TimeUnit.MILLISECONDS);
  }

  /** Called by {@link ClusterService} for frames of type REQUEST or RESPONSE. */
  void onFrame(byte type, DataInputStream in) throws IOException {
    String from = in.readUTF();
    long id = in.readLong();
    if (type == RESPONSE) {
      byte status = in.readByte();
      byte[] payload = in.readNBytes(in.readInt());
      CompletableFuture<Reply> reply = pending.remove(id);
      if (reply != null) {
        reply.complete(new Reply(status, payload));
      }
      return;
    }
    byte action = in.readByte();
    byte[] payload = in.readNBytes(in.readInt());
    // A resent request gets the first run's answer again instead of running twice.
    CompletableFuture<Reply> result = handled.get(from + ":" + id, k -> run(from, action, payload));
    result.whenComplete((r, ex) -> respond(from, id, ex == null ? r : Reply.of(FAILED)));
  }

  private CompletableFuture<Reply> run(String from, byte action, byte[] payload) {
    Handler handler = handlers.get(action);
    if (handler == null) {
      return CompletableFuture.completedFuture(Reply.of(UNKNOWN_ACTION));
    }
    try {
      return handler.handle(from, decode(payload));
    } catch (IOException | RuntimeException e) {
      logger.warn("Network request {} from {} failed", action, from, e);
      return CompletableFuture.completedFuture(Reply.of(FAILED));
    }
  }

  private void respond(String to, long id, Reply reply) {
    store.publish("msg:" + to, encode(out -> {
      out.writeByte(RESPONSE);
      out.writeUTF(proxyId);
      out.writeLong(id);
      out.writeByte(reply.status());
      out.writeInt(reply.payload().length);
      out.write(reply.payload());
    })).exceptionally(ex -> null); // the asker resends; we answer again from the cache
  }
}
