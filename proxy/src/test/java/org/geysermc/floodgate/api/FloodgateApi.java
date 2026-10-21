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

package org.geysermc.floodgate.api;

import java.util.Set;
import java.util.UUID;

/**
 * Test double of Floodgate's API, the two methods the proxy calls.
 */
public final class FloodgateApi {

  public static final Set<UUID> BEDROCK = new java.util.HashSet<>();
  private static final FloodgateApi INSTANCE = new FloodgateApi();

  public static FloodgateApi getInstance() {
    return INSTANCE;
  }

  public boolean isFloodgatePlayer(UUID uuid) {
    return BEDROCK.contains(uuid);
  }
}
