/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.event.network;

import com.google.common.base.Preconditions;
import com.velocitypowered.api.network.NetworkPlayer;
import java.util.Optional;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: fired on every proxy of the network when a player on any of them has connected to a
 * backend server. Delivery is best effort, see {@link NetworkPlayerJoinEvent}. Velocity does not
 * wait on this event.
 */
public final class NetworkPlayerServerChangeEvent {

  private final NetworkPlayer player;
  private final @Nullable String previousServer;

  public NetworkPlayerServerChangeEvent(NetworkPlayer player, @Nullable String previousServer) {
    this.player = Preconditions.checkNotNull(player, "player");
    this.previousServer = previousServer;
  }

  /**
   * Returns the player, with {@link NetworkPlayer#getServerName()} set to the new server.
   *
   * @return the player
   */
  public NetworkPlayer getPlayer() {
    return player;
  }

  public Optional<String> getPreviousServer() {
    return Optional.ofNullable(previousServer);
  }

  @Override
  public String toString() {
    return "NetworkPlayerServerChangeEvent{player=" + player + ", previousServer="
        + previousServer + '}';
  }
}
