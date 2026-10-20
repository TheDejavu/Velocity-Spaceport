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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Shared state for several proxies acting as one network. Every method is asynchronous and must
 * never be waited on from an event loop. Times ("seen", ages) come from the store's clock, so
 * proxies with skewed clocks still agree on who is alive.
 */
public interface NetworkStore extends AutoCloseable {

  /**
   * The heartbeat, run several times a second: records this proxy as alive with its player count,
   * creates the generation marker if it is missing, and returns every proxy's state.
   *
   * @param proxyId this proxy
   * @param players players on this proxy
   * @param servers backend servers this proxy has players on
   * @param instance this process (a random id), to notice another live proxy with the same id
   * @param draining whether this proxy is draining (takes no new players from the balancer)
   * @param ttlMillis how long this proxy's record lives without another tick
   * @param candidateGeneration generation to write if the store has none
   * @return the tick result
   */
  CompletableFuture<Tick> tick(String proxyId, int players, Collection<String> servers,
      String instance, boolean draining, long ttlMillis, String candidateGeneration);

  /**
   * A tick without servers and instance, for tests and tools.
   *
   * @param proxyId this proxy
   * @param players players on this proxy
   * @param ttlMillis how long this proxy's record lives without another tick
   * @param candidateGeneration generation to write if the store has none
   * @return the tick result
   */
  default CompletableFuture<Tick> tick(String proxyId, int players, long ttlMillis,
      String candidateGeneration) {
    return tick(proxyId, players, List.of(), "", false, ttlMillis, candidateGeneration);
  }

  /**
   * Reads one proxy's record.
   *
   * @param proxyId the proxy
   * @return its state, if it has a record
   */
  CompletableFuture<Optional<ProxyState>> proxy(String proxyId);

  /**
   * Takes ownership of a player for this proxy. A player owned by a proxy seen within
   * {@code deadAfterMillis} is not taken; one owned by a proxy not seen for longer is. A claim
   * never replaces one of this proxy's own with a higher epoch.
   *
   * @param player the record to write, {@code proxy} is this proxy
   * @param epoch this proxy's epoch
   * @param deadAfterMillis how long a proxy may be silent before its players can be taken
   * @param lastServer whether a player taken from a dead proxy is routed back to its server
   * @return the result
   */
  CompletableFuture<Claim> claim(PlayerRecord player, long epoch, long deadAfterMillis,
      boolean lastServer);

  /**
   * A claim without last-server routing.
   *
   * @param player the record to write
   * @param epoch this proxy's epoch
   * @param deadAfterMillis how long a proxy may be silent before its players can be taken
   * @return the result
   */
  default CompletableFuture<Claim> claim(PlayerRecord player, long epoch, long deadAfterMillis) {
    return claim(player, epoch, deadAfterMillis, false);
  }

  /**
   * Records the backend server of a player this proxy owns with this session.
   *
   * @param player the player
   * @param proxyId this proxy
   * @param session the login session
   * @param server the server name
   * @return true if the record was updated
   */
  CompletableFuture<Boolean> updateServer(UUID player, String proxyId, long session,
      String server);

  /**
   * Removes a player this proxy owns with this session.
   *
   * @param player the player
   * @param proxyId this proxy
   * @param session the login session
   * @return true if it was removed
   */
  CompletableFuture<Boolean> release(UUID player, String proxyId, long session);

  /**
   * Removes a proxy and every player it still owns, if it has not been seen for
   * {@code deadAfterMillis} or {@code force} is set.
   *
   * @param proxyId the proxy
   * @param deadAfterMillis how long it must have been silent
   * @param force remove even if alive (a proxy leaving on shutdown)
   * @param lastServerTtlMillis how long removed players are routed back to their server (0: not)
   * @return the number of players removed, or -1 if the proxy is alive
   */
  CompletableFuture<Integer> sweep(String proxyId, long deadAfterMillis, boolean force,
      long lastServerTtlMillis);

  CompletableFuture<Optional<PlayerRecord>> player(UUID player);

  CompletableFuture<Optional<UUID>> uuidOf(String name);

  /**
   * Returns a number higher than any returned before, unless the store lost its data.
   *
   * @return the counter
   */
  CompletableFuture<Long> nextEpoch();

  /**
   * Remembers where a player is going (a transfer), for {@link #claim} on the next proxy.
   *
   * @param player the player
   * @param server the backend server
   * @param ttlMillis how long the hint lives
   * @return completes when written
   */
  CompletableFuture<Void> putRoute(UUID player, String server, long ttlMillis);

  /**
   * Uses up a transfer ticket's nonce.
   *
   * @param nonce the nonce
   * @param ttlMillis how long to remember it (at least the ticket's lifetime)
   * @return true the first time, false if it was used before
   */
  CompletableFuture<Boolean> useNonce(String nonce, long ttlMillis);

  /**
   * Caches a player's profile for every proxy.
   *
   * @param profile the profile
   * @param ttlMillis how long it stays
   * @return completes when written
   */
  CompletableFuture<Void> putProfile(CachedProfile profile, long ttlMillis);

  CompletableFuture<Optional<CachedProfile>> profile(UUID player);

  CompletableFuture<Optional<CachedProfile>> profile(String name);

  /**
   * A player's profile in the shared cache.
   *
   * @param uuid the player
   * @param name its name
   * @param properties its profile properties (skin) as JSON
   */
  record CachedProfile(UUID uuid, String name, String properties) {
  }

  CompletableFuture<Void> publish(String channel, byte[] payload);

  CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler);

  @Override
  void close();

  /**
   * A player's record.
   *
   * @param uuid the player
   * @param name the player's name
   * @param proxy the owning proxy
   * @param session the owning login session, unique per login
   * @param server the backend server, or null
   * @param ip the player's address
   * @param since login time, epoch millis
   */
  record PlayerRecord(UUID uuid, String name, String proxy, long session, @Nullable String server,
                      String ip, long since) {
  }

  /**
   * Outcome of {@link #claim}.
   *
   * @param claimed whether this proxy owns the player now
   * @param owner the live owner if not claimed, else the previous owner or null
   * @param route where the player should go (a transfer hint or the last server of a crashed
   *     proxy), or null; taken from the store with the claim
   */
  record Claim(boolean claimed, @Nullable String owner, @Nullable String route) {
    public Claim(boolean claimed, @Nullable String owner) {
      this(claimed, owner, null);
    }
  }

  /**
   * A proxy as seen by a tick.
   *
   * @param id the proxy id
   * @param players its player count, -1 if its record is gone
   * @param ageMillis time since it was last seen, -1 if its record is gone
   * @param servers backend servers it has players on
   * @param instance the process that wrote the record
   * @param draining whether it is draining
   */
  record ProxyState(String id, int players, long ageMillis, List<String> servers,
                    String instance, boolean draining) {
  }

  /**
   * Outcome of {@link #tick}.
   *
   * @param generation the store's generation marker
   * @param generationCreated whether this tick created it
   * @param selfExisted whether this proxy's record existed before the tick
   * @param previousInstance the instance that wrote this proxy's record before, or empty
   * @param proxies every proxy the store knows, this one included
   */
  record Tick(String generation, boolean generationCreated, boolean selfExisted,
              String previousInstance, List<ProxyState> proxies) {
  }
}
