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

/**
 * A {@link NetworkPlayer} from a store record.
 *
 * @param record the record
 */
public record VelocityNetworkPlayer(PlayerRecord record) implements NetworkPlayer {

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
  public String toString() {
    return "NetworkPlayer{" + record.name() + " " + record.uuid() + " on " + record.proxy()
        + (record.server() == null ? "" : "/" + record.server()) + '}';
  }
}
