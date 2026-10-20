/*
 * Copyright (C) 2026 Velocity Contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.velocitypowered.proxy.network.cluster;

import com.velocitypowered.api.network.NetworkPlayer;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link NetworkPlayer} from a store record; its actions go to wherever the player is.
 */
public final class VelocityNetworkPlayer implements NetworkPlayer {

  /** Carries out the actions, locally or across the network. */
  public interface Actions {
    CompletableFuture<Boolean> message(UUID player, Component message);

    CompletableFuture<Boolean> connect(UUID player, String server);

    CompletableFuture<Boolean> disconnect(UUID player, Component reason);
  }

  private final PlayerRecord record;
  private final @Nullable Actions actions;

  public VelocityNetworkPlayer(PlayerRecord record, @Nullable Actions actions) {
    this.record = record;
    this.actions = actions;
  }

  public PlayerRecord record() {
    return record;
  }

  @Override
  public UUID getUniqueId() {
    return record.uuid();
  }

  @Override
  public String getUsername() {
    return record.name();
  }

  @Override
  public String getProxyId() {
    return record.proxy();
  }

  @Override
  public Optional<String> getServerName() {
    return Optional.ofNullable(record.server());
  }

  @Override
  public Instant getLoginTime() {
    return Instant.ofEpochMilli(record.since());
  }

  @Override
  public CompletableFuture<Boolean> sendMessage(Component message) {
    return actions == null ? CompletableFuture.completedFuture(false)
        : actions.message(record.uuid(), message);
  }

  @Override
  public CompletableFuture<Boolean> connect(String server) {
    return actions == null ? CompletableFuture.completedFuture(false)
        : actions.connect(record.uuid(), server);
  }

  @Override
  public CompletableFuture<Boolean> disconnect(Component reason) {
    return actions == null ? CompletableFuture.completedFuture(false)
        : actions.disconnect(record.uuid(), reason);
  }

  @Override
  public String toString() {
    return "NetworkPlayer{" + record.name() + " " + record.uuid() + " on " + record.proxy()
        + (record.server() == null ? "" : "/" + record.server()) + '}';
  }
}
