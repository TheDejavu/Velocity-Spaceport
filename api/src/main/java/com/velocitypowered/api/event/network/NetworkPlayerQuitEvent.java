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
 * Spaceport: fired on every proxy of the network when a player leaves the network. Delivery is
 * best effort, see {@link NetworkPlayerJoinEvent}. Velocity does not wait on this event.
 */
public final class NetworkPlayerQuitEvent {

  private final NetworkPlayer player;

  public NetworkPlayerQuitEvent(NetworkPlayer player) {
    this.player = Preconditions.checkNotNull(player, "player");
  }

  public NetworkPlayer getPlayer() {
    return player;
  }

  @Override
  public String toString() {
    return "NetworkPlayerQuitEvent{player=" + player + '}';
  }
}
