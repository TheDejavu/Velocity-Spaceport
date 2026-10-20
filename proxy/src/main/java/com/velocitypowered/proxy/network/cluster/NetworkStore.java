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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Shared state for several proxies acting as one network. Every method is asynchronous and must
 * never be waited on from an event loop.
 */
public interface NetworkStore extends AutoCloseable {

  /**
   * Marks this proxy alive for {@code ttlMillis}.
   *
   * @param proxyId this proxy
   * @param ttlMillis how long the mark lives without another heartbeat
   * @return completes when written
   */
  CompletableFuture<Void> heartbeat(String proxyId, long ttlMillis);

  /**
   * Takes ownership of a player for this proxy. A player owned by a live proxy is not taken, a
   * player owned by a dead proxy is.
   *
   * @param player the player
   * @param proxyId this proxy
   * @param epoch this proxy's epoch, a claim never replaces one of its own with a higher epoch
   * @return the result
   */
  CompletableFuture<ClaimResult> claim(UUID player, String proxyId, long epoch);

  /**
   * Gives up ownership of a player if this proxy still owns it with this epoch.
   *
   * @param player the player
   * @param proxyId this proxy
   * @param epoch this proxy's epoch
   * @return true if the claim was removed
   */
  CompletableFuture<Boolean> release(UUID player, String proxyId, long epoch);

  /**
   * Returns a new epoch, higher than any returned before.
   *
   * @return the epoch
   */
  CompletableFuture<Long> nextEpoch();

  CompletableFuture<Void> publish(String channel, byte[] payload);

  CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler);

  @Override
  void close();

  /**
   * Outcome of {@link #claim}.
   *
   * @param claimed whether this proxy owns the player now
   * @param owner the proxy that owns the player if not claimed, or the dead proxy it was taken
   *     from, or null
   */
  record ClaimResult(boolean claimed, @Nullable String owner) {
  }
}
