/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

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
}
