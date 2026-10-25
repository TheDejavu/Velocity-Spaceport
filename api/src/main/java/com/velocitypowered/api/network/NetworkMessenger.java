/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.key.Key;

/**
 * Spaceport: messages between plugins on the proxies of a network, on channels named by a
 * namespaced {@link Key} (for example {@code myplugin:chat}).
 *
 * <p>A message goes to one proxy ({@link #send}) or to all other proxies ({@link #broadcast}), or
 * is a request that one proxy answers ({@link #request}). Delivery is acknowledged and retried
 * for a few seconds, and a retried message reaches a listener only once, but there is no ordering
 * between messages and nothing is stored for a proxy that is down. Payloads are at most
 * {@value #MAX_PAYLOAD_BYTES} bytes; the format is up to the plugins (versions of a plugin on
 * different proxies must understand each other).
 *
 * <p>Listeners and handlers run on a pool of threads of the proxy, never on a network thread, so
 * they may block briefly, but should return soon. Exceptions they throw are logged.
 *
 * <p>Without a network ({@link Network#isEnabled()} is false) the messenger still works for
 * this proxy's own id: messages to it are delivered to local listeners, other proxies are
 * unknown.
 *
 * @since 4.3.1
 */
public interface NetworkMessenger {

  /**
   * The largest payload of a message, request or answer, in bytes.
   *
   * @since 4.3.1
   */
  int MAX_PAYLOAD_BYTES = 32 * 1024;

  /**
   * The longest timeout of a request.
   *
   * @since 4.3.1
   */
  Duration MAX_TIMEOUT = Duration.ofSeconds(60);

  /**
   * Receives messages sent to a channel.
   *
   * @since 4.3.1
   */
  @FunctionalInterface
  interface Listener {
    /**
     * Called for a message sent to the channel.
     *
     * @param channel the channel
     * @param fromProxyId the id of the proxy that sent it
     * @param payload the payload
     */
    void onMessage(Key channel, String fromProxyId, byte[] payload);
  }

  /**
   * Answers requests sent to a channel.
   *
   * @since 4.3.1
   */
  @FunctionalInterface
  interface RequestHandler {
    /**
     * Called for a request sent to the channel.
     *
     * @param channel the channel
     * @param fromProxyId the id of the proxy that asked
     * @param payload the request
     * @return the answer, at most {@link #MAX_PAYLOAD_BYTES} bytes; null is an empty answer
     * @throws Exception if the request cannot be answered; the asker gets a failed future
     */
    byte[] onRequest(Key channel, String fromProxyId, byte[] payload) throws Exception;
  }

  /**
   * Listens to messages on a channel, on this proxy. A channel can have several listeners, also
   * of several plugins.
   *
   * @param plugin the plugin instance that owns the listener
   * @param channel the channel
   * @param listener the listener
   * @since 4.3.1
   */
  void registerListener(Object plugin, Key channel, Listener listener);

  /**
   * Answers requests on a channel, on this proxy. A channel has at most one handler per proxy.
   *
   * @param plugin the plugin instance that owns the handler
   * @param channel the channel
   * @param handler the handler
   * @throws IllegalStateException if the channel already has a handler on this proxy
   * @since 4.3.1
   */
  void registerRequestHandler(Object plugin, Key channel, RequestHandler handler);

  /**
   * Removes the listeners and the request handler of a plugin on a channel.
   *
   * @param plugin the plugin instance
   * @param channel the channel
   * @since 4.3.1
   */
  void unregister(Object plugin, Key channel);

  /**
   * Removes everything a plugin registered. Call it when the plugin shuts down.
   *
   * @param plugin the plugin instance
   * @since 4.3.1
   */
  void unregisterAll(Object plugin);

  /**
   * Sends a message to one proxy, which may be this one.
   *
   * @param channel the channel
   * @param proxyId the id of the receiving proxy
   * @param payload the payload, at most {@link #MAX_PAYLOAD_BYTES} bytes
   * @return true if a listener on that proxy got it; false if the proxy is unknown, has no
   *     listener on the channel, or dropped the message because the channel had too many
   *     messages waiting; fails with a {@link java.util.concurrent.TimeoutException} if the proxy
   *     did not answer. Local and remote failures use the same exception types, never wrapped in
   *     a {@code CompletionException}
   * @throws IllegalArgumentException if the payload is too large
   * @since 4.3.1
   */
  CompletableFuture<Boolean> send(Key channel, String proxyId, byte[] payload);

  /**
   * Sends a message to every other live proxy of the network, not to this one.
   *
   * @param channel the channel
   * @param payload the payload, at most {@link #MAX_PAYLOAD_BYTES} bytes
   * @return the number of proxies on which a listener got it
   * @throws IllegalArgumentException if the payload is too large
   * @since 4.3.1
   */
  CompletableFuture<Integer> broadcast(Key channel, byte[] payload);

  /**
   * Asks a proxy, which may be this one, and waits for the answer of its request handler.
   *
   * @param channel the channel
   * @param proxyId the id of the proxy to ask
   * @param payload the request, at most {@link #MAX_PAYLOAD_BYTES} bytes
   * @param timeout how long to wait, at most {@link #MAX_TIMEOUT}
   * @return the answer; fails with a {@link java.util.concurrent.TimeoutException} if none came
   *     in time, or an {@link IllegalStateException} if the proxy has no handler on the channel
   *     or the handler failed, or an {@link IllegalArgumentException} if the proxy is unknown
   * @throws IllegalArgumentException if the payload is too large or the timeout is not positive
   * @since 4.3.1
   */
  CompletableFuture<byte[]> request(Key channel, String proxyId, byte[] payload,
      Duration timeout);
}
