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
}
