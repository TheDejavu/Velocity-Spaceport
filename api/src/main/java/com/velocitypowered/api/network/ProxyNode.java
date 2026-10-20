/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

/**
 * Spaceport: a proxy in the network.
 */
public interface ProxyNode {

  String getId();

  /**
   * Returns the number of players on this proxy, as last reported.
   *
   * @return the player count
   */
  int getPlayerCount();
}
