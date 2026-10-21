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
 * Spaceport: tells Bedrock players (through Geyser) apart from Java ones, without depending on
 * Geyser or Floodgate. A Java Transfer packet sent to a Bedrock player makes Geyser drop it, so
 * drains treat Bedrock players like clients too old to transfer.
 *
 * <ol>
 *   <li>Floodgate gives Bedrock players uuids whose upper 64 bits are zero (the lower half is the
 *   XUID); Mojang and offline uuids never look like that.</li>
 *   <li>If the Floodgate or Geyser plugin is loaded, its API is asked as well (found through the
 *   plugin's class loader, by reflection).</li>
 * </ol>
 */
final class BedrockDetector {

  private static final Logger logger = LogManager.getLogger(BedrockDetector.class);

  /** One plugin API check: the API object and its "is this a Bedrock player" method. */
  private record Check(Object api, Method method) {
  }

  private final Function<String, Optional<ClassLoader>> pluginLoader;
  private volatile @Nullable Check floodgate;
  private volatile @Nullable Check geyser;
  private volatile boolean resolved;

  /**
   * Creates the detector.
   *
   * @param pluginLoader the class loader of a loaded plugin by id, or empty if it is not loaded
   */
  BedrockDetector(Function<String, Optional<ClassLoader>> pluginLoader) {
    this.pluginLoader = pluginLoader;
  }

  boolean isBedrock(UUID player) {
    if (player.getMostSignificantBits() == 0) {
      return true;
    }
    if (!resolved) {
      floodgate = resolve("floodgate", "org.geysermc.floodgate.api.FloodgateApi", "getInstance",
          "isFloodgatePlayer");
      geyser = resolve("geyser", "org.geysermc.geyser.api.GeyserApi", "api",
          "isBedrockPlayer");
      resolved = true;
    }
    return ask(floodgate, player) || ask(geyser, player);
  }

  private @Nullable Check resolve(String pluginId, String apiClass, String getter, String test) {
    Optional<ClassLoader> loader = pluginLoader.apply(pluginId);
    if (loader.isEmpty()) {
      return null;
    }
    try {
      Class<?> type = Class.forName(apiClass, true, loader.get());
      Object api = type.getMethod(getter).invoke(null);
      return new Check(api, type.getMethod(test, UUID.class));
    } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
      logger.warn("Could not use the {} API to recognise Bedrock players: {}", pluginId,
          e.toString());
      return null;
    }
  }

  private static boolean ask(@Nullable Check check, UUID player) {
    if (check == null) {
      return false;
    }
    try {
      return Boolean.TRUE.equals(check.method().invoke(check.api(), player));
    } catch (ReflectiveOperationException | RuntimeException e) {
      return false;
    }
  }
}
