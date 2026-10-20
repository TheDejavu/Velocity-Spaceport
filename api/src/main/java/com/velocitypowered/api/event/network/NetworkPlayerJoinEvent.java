/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.event.network;

import com.google.common.base.Preconditions;
import com.velocitypowered.api.network.NetworkPlayer;

/**
 * Spaceport: fired on every proxy of the network when a player logs in to any of them. Delivery is
 * best effort (it may be missed while the network store fails over); the network state itself is
 * not. Velocity does not wait on this event.
 */
public final class NetworkPlayerJoinEvent {

  private final NetworkPlayer player;

  public NetworkPlayerJoinEvent(NetworkPlayer player) {
    this.player = Preconditions.checkNotNull(player, "player");
  }

  public NetworkPlayer getPlayer() {
    return player;
  }

  @Override
  public String toString() {
    return "NetworkPlayerJoinEvent{player=" + player + '}';
  }
}
