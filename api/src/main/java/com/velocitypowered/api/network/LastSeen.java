/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * The Velocity API is licensed under the terms of the MIT License. For more details,
 * reference the LICENSE file in the api top-level directory.
 */

package com.velocitypowered.api.network;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: where and when a player was last on the network. Recorded when a player leaves and
 * kept for the configured {@code last-seen-ttl}; for a player that is online right now the time is
 * the moment of the lookup and {@link #isOnline()} is true.
 *
 * @since 4.3.1
 */
public final class LastSeen {

  private final UUID uniqueId;
  private final String username;
  private final String proxyId;
  private final @Nullable String serverName;
  private final Instant time;
  private final boolean online;

  /**
   * Creates a record.
   *
   * @param uniqueId the player's uuid
   * @param username the player's name
   * @param proxyId the proxy the player was on
   * @param serverName the backend server the player was on, or null if on none
   * @param time when the player was seen
   * @param online whether the player is online now
   */
  public LastSeen(UUID uniqueId, String username, String proxyId, @Nullable String serverName,
      Instant time, boolean online) {
    this.uniqueId = Preconditions.checkNotNull(uniqueId, "uniqueId");
    this.username = Preconditions.checkNotNull(username, "username");
    this.proxyId = Preconditions.checkNotNull(proxyId, "proxyId");
    this.serverName = serverName;
    this.time = Preconditions.checkNotNull(time, "time");
    this.online = online;
  }

  /**
   * Returns the player's uuid.
   *
   * @return the uuid
   * @since 4.3.1
   */
  public UUID getUniqueId() {
    return uniqueId;
  }

  /**
   * Returns the player's name as it was then.
   *
   * @return the name
   * @since 4.3.1
   */
  public String getUsername() {
    return username;
  }

  /**
   * Returns the id of the proxy the player was last on.
   *
   * @return the proxy id
   * @since 4.3.1
   */
  public String getProxyId() {
    return proxyId;
  }

  /**
   * Returns the backend server the player was last on.
   *
   * @return the server name, empty if the player was not on a backend server
   * @since 4.3.1
   */
  public Optional<String> getServerName() {
    return Optional.ofNullable(serverName);
  }

  /**
   * Returns when the player was last seen: when it left, or now if it is online.
   *
   * @return the time
   * @since 4.3.1
   */
  public Instant getTime() {
    return time;
  }

  /**
   * Returns whether the player is online now.
   *
   * @return true if online
   * @since 4.3.1
   */
  public boolean isOnline() {
    return online;
  }

  @Override
  public String toString() {
    return "LastSeen{uniqueId=" + uniqueId + ", username=" + username + ", proxyId=" + proxyId
        + ", serverName=" + serverName + ", time=" + time + ", online=" + online + '}';
  }
}
