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

package com.velocitypowered.proxy.network.health;

import java.lang.reflect.Method;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Asks the Geyser plugin to send a Bedrock client to another Geyser. A Java transfer packet
 * never reaches that client.
 */
final class BedrockHandoff {

  private static final Logger logger = LogManager.getLogger(BedrockHandoff.class);

  private final Function<String, Optional<ClassLoader>> pluginLoader;
  private volatile boolean resolved;
  private volatile @Nullable Object api;
  private volatile @Nullable Method transfer;

  BedrockHandoff(Function<String, Optional<ClassLoader>> pluginLoader) {
    this.pluginLoader = pluginLoader;
  }

  boolean available() {
    resolve();
    return transfer != null;
  }

  /**
   * Moves the player.
   *
   * @param player the Java uuid Geyser knows
   * @param host the other Geyser
   * @param port its Bedrock port
   * @return whether Geyser accepted the move
   */
  boolean transfer(UUID player, String host, int port) {
    resolve();
    if (transfer == null || api == null) {
      return false;
    }
    try {
      return Boolean.TRUE.equals(transfer.invoke(api, player, host, port));
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.warn("Could not move Bedrock player {} to {}:{}: {}", player, host, port, e.toString());
      return false;
    }
  }

  private void resolve() {
    if (resolved) {
      return;
    }
    resolved = true;
    Optional<ClassLoader> loader = pluginLoader.apply("geyser");
    if (loader.isEmpty()) {
      return;
    }
    try {
      Class<?> type = Class.forName("org.geysermc.geyser.api.GeyserApi", true, loader.get());
      api = type.getMethod("api").invoke(null);
      transfer = type.getMethod("transfer", UUID.class, String.class, int.class);
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      logger.warn("Could not use the Geyser API to move Bedrock players: {}", e.toString());
    }
  }
}
