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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;
import org.geysermc.floodgate.api.FloodgateApi;
import org.junit.jupiter.api.Test;

class BedrockDetectorTest {

  @Test
  void floodgateUuidsAreBedrockWithoutAnyPlugin() {
    BedrockDetector detector = new BedrockDetector(id -> Optional.empty());
    assertTrue(detector.isBedrock(new UUID(0, 2535428650957117L))); // a XUID
    assertFalse(detector.isBedrock(UUID.randomUUID()));
    assertFalse(detector.isBedrock(UUID.nameUUIDFromBytes("OfflinePlayer:Steve".getBytes())));
  }

  @Test
  void floodgateApiIsAskedWhenThePluginIsLoaded() {
    UUID linked = UUID.randomUUID(); // a Bedrock player linked to a Java account
    FloodgateApi.BEDROCK.add(linked);
    try {
      BedrockDetector detector = new BedrockDetector(id -> id.equals("floodgate")
          ? Optional.of(getClass().getClassLoader()) : Optional.empty());
      assertTrue(detector.isBedrock(linked));
      assertFalse(detector.isBedrock(UUID.randomUUID()));
    } finally {
      FloodgateApi.BEDROCK.clear();
    }
  }

  @Test
  void brokenPluginApiIsIgnored() {
    // "geyser" is "loaded" but its API class is missing: no exception, just the uuid rule
    BedrockDetector detector = new BedrockDetector(id -> Optional.of(getClass().getClassLoader()));
    assertFalse(detector.isBedrock(UUID.randomUUID()));
  }
}
