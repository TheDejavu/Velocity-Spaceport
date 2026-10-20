/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Spaceport: a player somewhere on the network, as recorded when it was looked up.
 */
public interface NetworkPlayer {

  UUID getUniqueId();

  String getUsername();

  /**
   * Returns the id of the proxy the player is connected to.
   *
   * @return the proxy id
   */
  String getProxyId();

  /**
   * Returns the name of the backend server the player is on, if connected to one.
   *
   * @return the server name
   */
  Optional<String> getServerName();

  /**
   * Returns when the player logged in to its proxy.
   *
   * @return the login time
   */
  Instant getLoginTime();
}
