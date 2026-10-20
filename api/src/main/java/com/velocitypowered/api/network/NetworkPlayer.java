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
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;

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

  /**
   * Sends a chat message to the player, on whichever proxy it is.
   *
   * @param message the message
   * @return true if the player was found and got it
   */
  CompletableFuture<Boolean> sendMessage(Component message);

  /**
   * Connects the player to a backend server, on whichever proxy it is.
   *
   * @param server the server name
   * @return true if the player is now on that server
   */
  CompletableFuture<Boolean> connect(String server);

  /**
   * Disconnects the player from the network.
   *
   * @param reason the reason shown to the player
   * @return true if the player was found and disconnected
   */
  CompletableFuture<Boolean> disconnect(Component reason);
}
