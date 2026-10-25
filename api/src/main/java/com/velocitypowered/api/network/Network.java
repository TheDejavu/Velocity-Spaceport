/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

import com.velocitypowered.api.util.GameProfile;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;

/**
 * Spaceport: the network of proxies this proxy belongs to. When the network is disabled it
 * describes this proxy alone.
 */
public interface Network {

  /**
   * Returns whether this proxy shares its players with other proxies.
   *
   * @return true if the network is enabled
   */
  boolean isEnabled();

  /**
   * Returns this proxy's id in the network.
   *
   * @return the proxy id
   */
  String getProxyId();

  /**
   * Returns the number of players on the whole network. The value is cached and refreshed several
   * times a second; it never waits on the network store.
   *
   * @return the network player count
   */
  int getPlayerCount();

  /**
   * Returns the live proxies of the network, as last seen.
   *
   * @return the proxies, including this one
   */
  Collection<ProxyNode> getProxies();

  /**
   * Looks up a player anywhere on the network.
   *
   * @param uuid the player's uuid
   * @return the player, if online
   */
  CompletableFuture<Optional<NetworkPlayer>> getPlayer(UUID uuid);

  /**
   * Looks up a player anywhere on the network by name, ignoring case.
   *
   * @param username the player's name
   * @return the player, if online
   */
  CompletableFuture<Optional<NetworkPlayer>> getPlayer(String username);

  /**
   * Lists every player on the network by asking every proxy. Players on a proxy that does not
   * answer in time are missing.
   *
   * @return the players
   */
  CompletableFuture<Collection<NetworkPlayer>> getPlayers();

  /**
   * Sends a chat message to every player on the network.
   *
   * @param message the message
   * @return completes when every proxy got it, or failed
   */
  CompletableFuture<Void> broadcast(Component message);

  /**
   * Runs a command as the console of a proxy of the network.
   *
   * @param proxyId the proxy
   * @param command the command line, without a leading slash
   * @return true if the proxy ran it and the command succeeded
   */
  CompletableFuture<Boolean> executeCommand(String proxyId, String command);

  /**
   * Returns whether this proxy is draining.
   *
   * @return true if draining
   */
  boolean isDraining();

  /**
   * Starts draining this proxy: its health check turns not ready at once so the load balancer
   * stops sending players, and after the configured health interval it is marked draining in the
   * network. Players that still arrive are let in.
   *
   * @return completes when this proxy is marked draining in the network
   */
  CompletableFuture<Void> drain();

  /**
   * Stops a drain; the proxy takes new players again.
   */
  void cancelDrain();

  /**
   * Looks up a player's profile (uuid, name, skin) in the network's shared cache, filled by every
   * login on every proxy. Players offline for longer than the cache lifetime are not found.
   *
   * @param uuid the player
   * @return the cached profile, if any
   */
  CompletableFuture<Optional<GameProfile>> getCachedProfile(UUID uuid);

  /**
   * Looks up a player's profile by name, ignoring case, in the network's shared cache.
   *
   * @param username the name
   * @return the cached profile, if any
   */
  CompletableFuture<Optional<GameProfile>> getCachedProfile(String username);

  /**
   * Returns the number of players on a backend server across the whole network. Like
   * {@link #getPlayerCount()} it is cached, refreshed several times a second, and never waits on
   * the network store.
   *
   * @param serverName the backend server's name, ignoring case
   * @return the players on that server on all proxies, 0 for an unknown server
   * @since 4.3.1
   */
  int getPlayerCount(String serverName);

  /**
   * Returns the number of players on one proxy, as last reported.
   *
   * @param proxyId the proxy's id
   * @return the players on it, 0 if there is no such live proxy
   * @since 4.3.1
   */
  int getPlayerCountOnProxy(String proxyId);

  /**
   * Returns one live proxy of the network, as last seen.
   *
   * @param proxyId the proxy's id
   * @return the proxy, if it is alive
   * @since 4.3.1
   */
  Optional<ProxyNode> getProxy(String proxyId);

  /**
   * Lists the players on a backend server across the network. Only the proxies that have players
   * on that server are asked; one that does not answer in time is missing.
   *
   * @param serverName the backend server's name, ignoring case
   * @return the players on it
   * @since 4.3.1
   */
  CompletableFuture<Collection<NetworkPlayer>> getPlayers(String serverName);

  /**
   * Lists the players of one proxy.
   *
   * @param proxyId the proxy's id
   * @return its players; empty if the proxy is unknown or does not answer in time
   * @since 4.3.1
   */
  CompletableFuture<Collection<NetworkPlayer>> getPlayersOnProxy(String proxyId);

  /**
   * Checks whether a player is online anywhere on the network. A single store lookup, cheaper than
   * {@link #getPlayer(UUID)}.
   *
   * @param uuid the player's uuid
   * @return true if the player is online
   * @since 4.3.1
   */
  CompletableFuture<Boolean> isOnline(UUID uuid);

  /**
   * Checks whether a player is online anywhere on the network, by name, ignoring case.
   *
   * @param username the player's name
   * @return true if the player is online
   * @since 4.3.1
   */
  CompletableFuture<Boolean> isOnline(String username);

  /**
   * Finds a player's uuid by name, ignoring case. Looks at players online now, then at the shared
   * profile cache, then at the last-seen records, so it also finds players that left some time
   * ago. Without a network, only this proxy's players and the players it has seen are known.
   *
   * @param username the player's name
   * @return the uuid, if the name is known
   * @since 4.3.1
   */
  CompletableFuture<Optional<UUID>> getUniqueId(String username);

  /**
   * Finds a player's name by uuid, with the same sources as {@link #getUniqueId(String)}.
   *
   * @param uuid the player's uuid
   * @return the name as last used, if the player is known
   * @since 4.3.1
   */
  CompletableFuture<Optional<String>> getUsername(UUID uuid);

  /**
   * Looks up where and when a player was last on the network. For a player that is online it
   * answers with the current proxy and server and the current time. Records are written when a
   * player leaves, shared by all proxies and kept for {@code last-seen-ttl} days (0 turns them
   * off); without a network they are kept in memory on this proxy only.
   *
   * @param uuid the player's uuid
   * @return the record, if the player is online or was seen within the retention time
   * @since 4.3.1
   */
  CompletableFuture<Optional<LastSeen>> getLastSeen(UUID uuid);

  /**
   * Looks up where and when a player was last on the network, by name, ignoring case.
   *
   * @param username the player's name
   * @return the record, if the player is online or was seen within the retention time
   * @see #getLastSeen(UUID)
   * @since 4.3.1
   */
  CompletableFuture<Optional<LastSeen>> getLastSeen(String username);

  /**
   * Returns the backend server a player is on, or was on when it last left.
   *
   * @param uuid the player's uuid
   * @return the server name, if known
   * @since 4.3.1
   */
  CompletableFuture<Optional<String>> getLastServer(UUID uuid);

  /**
   * Returns the messenger for plugin messages between proxies.
   *
   * @return the messenger
   * @since 4.3.1
   */
  NetworkMessenger getMessenger();
}
