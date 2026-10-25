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

import com.velocitypowered.api.network.NetworkMessenger;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.network.cluster.MessageBus.Reply;
import java.io.DataInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.kyori.adventure.key.Key;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link NetworkMessenger}: plugin channels over the {@link MessageBus} (one request action), so
 * a message is acknowledged, resent when lost and run once. Listeners and handlers run on a small
 * pool of their own, so the thread that reads the store's pub/sub is never held up by a plugin.
 * Registrations are kept per plugin and per channel, and are removed by
 * {@link #unregisterAll(Object)}.
 */
public final class PluginMessenger implements NetworkMessenger {

  /** The bus action of plugin messages and requests. */
  static final byte PLUGIN = 9;
  private static final byte MESSAGE = 0;
  private static final byte REQUEST = 1;
  /**
   * Tasks of one channel that may wait or run at once; more are dropped, so one slow plugin
   * cannot fill the shared queue.
   */
  static final int CHANNEL_IN_FLIGHT_MAX = 256;
  private static final Reply EMPTY_FAILED = Reply.of(MessageBus.FAILED);

  private static final Logger logger = LogManager.getLogger(PluginMessenger.class);

  /** One registration: a listener or a request handler of a plugin. */
  private record Registration(Object plugin, @Nullable Listener listener,
                              @Nullable RequestHandler handler) {
  }

  private final @Nullable VelocityServer server;
  private final @Nullable ClusterService cluster;
  private final Map<Key, List<Registration>> channels = new ConcurrentHashMap<>();
  private final Map<Key, AtomicInteger> inFlight = new ConcurrentHashMap<>();
  private final AtomicInteger threads = new AtomicInteger();
  private final ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 30, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>(4096), r -> {
        Thread t = new Thread(r, "Spaceport Plugin Messages #" + threads.incrementAndGet());
        t.setDaemon(true);
        return t;
      });

  /**
   * Creates the messenger.
   *
   * @param server the proxy, to check that registering plugins exist; null skips the check
   * @param cluster the network, or null when it is off
   */
  public PluginMessenger(@Nullable VelocityServer server, @Nullable ClusterService cluster) {
    this.server = server;
    this.cluster = cluster;
    executor.allowCoreThreadTimeOut(true);
    if (cluster != null) {
      cluster.bus().register(PLUGIN, this::onBus);
    }
  }

  /** Stops the pool, for the proxy's shutdown. */
  public void close() {
    executor.shutdownNow();
  }

  private void checkPlugin(Object plugin) {
    if (plugin == null) {
      throw new NullPointerException("plugin");
    }
    if (server != null) {
      server.getPluginManager().ensurePluginContainer(plugin);
    }
  }

  @Override
  public void registerListener(Object plugin, Key channel, Listener listener) {
    checkPlugin(plugin);
    Registration added = new Registration(plugin, Objects.requireNonNull(listener, "listener"),
        null);
    channels.compute(channel, (k, list) -> append(list, added));
  }

  @Override
  public void registerRequestHandler(Object plugin, Key channel, RequestHandler handler) {
    checkPlugin(plugin);
    Registration added = new Registration(plugin, null,
        Objects.requireNonNull(handler, "handler"));
    channels.compute(channel, (k, list) -> {
      if (list != null && list.stream().anyMatch(r -> r.handler() != null)) {
        throw new IllegalStateException("channel " + channel.asString()
            + " already has a request handler");
      }
      return append(list, added);
    });
  }

  /** Registrations are replaced, never changed, so readers see a consistent list. */
  private static List<Registration> append(@Nullable List<Registration> list, Registration r) {
    List<Registration> next = new ArrayList<>(list == null ? List.of() : list);
    next.add(r);
    return List.copyOf(next);
  }

  @Override
  public void unregister(Object plugin, Key channel) {
    channels.computeIfPresent(channel, (k, list) -> {
      List<Registration> kept = list.stream().filter(r -> r.plugin() != plugin).toList();
      return kept.isEmpty() ? null : kept;
    });
  }

  @Override
  public void unregisterAll(Object plugin) {
    for (Key channel : List.copyOf(channels.keySet())) {
      unregister(plugin, channel);
    }
  }

  private static void checkPayload(byte[] payload) {
    if (payload.length > MAX_PAYLOAD_BYTES) {
      throw new IllegalArgumentException("payload of " + payload.length + " bytes is larger than "
          + MAX_PAYLOAD_BYTES);
    }
  }

  /** Runs a task of a channel on the pool, unless the channel has too many in flight. */
  private boolean submit(Key channel, Runnable task) {
    AtomicInteger count = inFlight.computeIfAbsent(channel, k -> new AtomicInteger());
    if (count.incrementAndGet() > CHANNEL_IN_FLIGHT_MAX) {
      count.decrementAndGet();
      logger.warn("Too many plugin messages in flight on {}; dropped one", channel.asString());
      return false;
    }
    try {
      executor.execute(() -> {
        try {
          task.run();
        } finally {
          count.decrementAndGet();
        }
      });
      return true;
    } catch (RejectedExecutionException e) {
      count.decrementAndGet();
      logger.warn("Plugin message queue is full; dropped a message on {}", channel.asString());
      return false;
    }
  }

  // Delivery on this proxy -----------------------------------------------------------------

  /**
   * Hands a message to the listeners of a channel on this proxy.
   *
   * @return the number of listeners, or -1 if the pool is full and nothing ran
   */
  private int deliver(Key channel, String from, byte[] payload) {
    List<Registration> list = channels.get(channel);
    if (list == null) {
      return 0;
    }
    int n = 0;
    for (Registration r : list) {
      Listener listener = r.listener();
      if (listener == null) {
        continue;
      }
      n++;
      byte[] copy = payload.clone(); // every listener gets its own, and may change it
      if (!submit(channel, () -> {
        try {
          listener.onMessage(channel, from, copy);
        } catch (Throwable t) {
          logger.warn("Listener of {} on {} failed", r.plugin().getClass().getName(),
              channel.asString(), t);
        }
      })) {
        return -1;
      }
    }
    return n;
  }

  private CompletableFuture<byte[]> answer(Key channel, String from, byte[] payload) {
    List<Registration> list = channels.get(channel);
    RequestHandler handler = list == null ? null : list.stream().map(Registration::handler)
        .filter(h -> h != null).findFirst().orElse(null);
    if (handler == null) {
      return CompletableFuture.failedFuture(new IllegalStateException(
          "no request handler on " + channel.asString()));
    }
    CompletableFuture<byte[]> result = new CompletableFuture<>();
    byte[] copy = payload.clone();
    if (!submit(channel, () -> {
      try {
        byte[] answer = handler.onRequest(channel, from, copy);
        answer = answer == null ? new byte[0] : answer.clone();
        if (answer.length > MAX_PAYLOAD_BYTES) {
          logger.warn("Handler of {} answered with {} bytes, more than {}", channel.asString(),
              answer.length, MAX_PAYLOAD_BYTES);
          result.completeExceptionally(new IllegalStateException("answer too large"));
        } else {
          result.complete(answer);
        }
      } catch (Throwable t) {
        logger.warn("Request handler on {} failed", channel.asString(), t);
        result.completeExceptionally(new IllegalStateException("handler failed"));
      }
    })) {
      result.completeExceptionally(new IllegalStateException("handler queue full"));
    }
    return result;
  }

  // From other proxies ---------------------------------------------------------------------

  private CompletableFuture<Reply> onBus(String from, DataInputStream in) throws IOException {
    Key channel = Key.key(in.readUTF());
    byte kind = in.readByte();
    int length = in.readInt();
    if (length < 0 || length > MAX_PAYLOAD_BYTES) {
      return CompletableFuture.completedFuture(EMPTY_FAILED);
    }
    byte[] payload = in.readNBytes(length);
    if (kind == MESSAGE) {
      int n = deliver(channel, from, payload);
      return CompletableFuture.completedFuture(n == 0 ? Reply.of(MessageBus.NO_HANDLER)
          : n < 0 ? EMPTY_FAILED : Reply.of(MessageBus.OK));
    }
    if (kind != REQUEST) {
      return CompletableFuture.completedFuture(Reply.of(MessageBus.UNKNOWN_ACTION));
    }
    if (!hasHandler(channel)) {
      return CompletableFuture.completedFuture(Reply.of(MessageBus.NO_HANDLER));
    }
    return answer(channel, from, payload).handle((bytes, ex) -> ex == null
        ? new Reply(MessageBus.OK, bytes) : EMPTY_FAILED);
  }

  private boolean hasHandler(Key channel) {
    List<Registration> list = channels.get(channel);
    return list != null && list.stream().anyMatch(r -> r.handler() != null);
  }

  // To other proxies -----------------------------------------------------------------------

  private String self() {
    return cluster == null ? "local" : cluster.proxyId();
  }

  private boolean isSelf(String proxyId) {
    return proxyId.equals(self());
  }

  /** Whether a proxy is alive: the last tick, else a look in the store. */
  private CompletableFuture<Boolean> alive(String proxyId) {
    if (cluster == null) {
      return CompletableFuture.completedFuture(false);
    }
    if (cluster.liveRemotes().stream().anyMatch(p -> p.id().equals(proxyId))) {
      return CompletableFuture.completedFuture(true);
    }
    return cluster.store().proxy(proxyId)
        .thenApply(p -> p.filter(s -> s.ageMillis() < ClusterService.DEAD_AFTER_MILLIS)
            .isPresent())
        .exceptionally(ex -> false);
  }

  private static byte[] frame(Key channel, byte kind, byte[] payload) {
    return MessageBus.encode(out -> {
      out.writeUTF(channel.asString());
      out.writeByte(kind);
      out.writeInt(payload.length);
      out.write(payload);
    });
  }

  /** Completes with the same exception types for local and remote failures. */
  private static <T> CompletableFuture<T> plain(CompletableFuture<T> future) {
    CompletableFuture<T> out = new CompletableFuture<>();
    future.whenComplete((v, ex) -> {
      if (ex == null) {
        out.complete(v);
      } else {
        Throwable cause = ex;
        while (cause instanceof CompletionException && cause.getCause() != null) {
          cause = cause.getCause();
        }
        out.completeExceptionally(cause);
      }
    });
    return out;
  }

  @Override
  public CompletableFuture<Boolean> send(Key channel, String proxyId, byte[] payload) {
    checkPayload(payload);
    if (isSelf(proxyId)) {
      return CompletableFuture.completedFuture(deliver(channel, self(), payload.clone()) > 0);
    }
    return plain(alive(proxyId).thenCompose(known -> {
      if (!known) {
        return CompletableFuture.completedFuture(false);
      }
      return cluster.bus().request(proxyId, PLUGIN, frame(channel, MESSAGE, payload))
          .thenApply(Reply::ok);
    }));
  }

  @Override
  public CompletableFuture<Integer> broadcast(Key channel, byte[] payload) {
    checkPayload(payload);
    if (cluster == null) {
      return CompletableFuture.completedFuture(0);
    }
    byte[] frame = frame(channel, MESSAGE, payload);
    List<CompletableFuture<Boolean>> sends = new ArrayList<>();
    for (NetworkStore.ProxyState p : cluster.liveRemotes()) {
      sends.add(cluster.bus().request(p.id(), PLUGIN, frame).thenApply(Reply::ok)
          .exceptionally(ex -> false));
    }
    return CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
        .thenApply(v -> (int) sends.stream().filter(CompletableFuture::join).count());
  }

  @Override
  public CompletableFuture<byte[]> request(Key channel, String proxyId, byte[] payload,
      Duration timeout) {
    checkPayload(payload);
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    long millis = Math.min(timeout.toMillis(), MAX_TIMEOUT.toMillis());
    if (isSelf(proxyId)) {
      return plain(answer(channel, self(), payload.clone())
          .orTimeout(millis, TimeUnit.MILLISECONDS));
    }
    return plain(alive(proxyId).thenCompose(known -> {
      if (!known) {
        return CompletableFuture.failedFuture(new IllegalArgumentException(
            "unknown proxy " + proxyId));
      }
      return cluster.bus().request(proxyId, PLUGIN, frame(channel, REQUEST, payload),
          Math.max(1, millis)).thenCompose(reply -> {
            if (reply.ok()) {
              return CompletableFuture.completedFuture(reply.payload());
            }
            return CompletableFuture.failedFuture(new IllegalStateException(
                reply.status() == MessageBus.NO_HANDLER
                    ? "proxy " + proxyId + " has no request handler on " + channel.asString()
                    : "the request handler on " + proxyId + " failed"));
          });
    }));
  }
}
