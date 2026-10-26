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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.velocitypowered.proxy.network.cluster.BedrockDoors.Door;
import com.velocitypowered.proxy.network.cluster.BedrockDoors.Offer;
import java.util.List;
import org.junit.jupiter.api.Test;

class BedrockDoorsTest {

  private static final Door A = new Door("geyser-a.example", 19132);
  private static final Door B = new Door("geyser-b.example", 19133);

  @Test
  void parseReadsHostAndPort() {
    assertEquals(A, BedrockDoors.parse("geyser-a.example:19132"));
    assertNull(BedrockDoors.parse(null));
    assertNull(BedrockDoors.parse("geyser-a.example"));
    assertNull(BedrockDoors.parse(":19132"));
    assertNull(BedrockDoors.parse("geyser-a.example:0"));
    assertNull(BedrockDoors.parse("geyser-a.example:nope"));
  }

  @Test
  void pickSkipsThisProxyAndAnythingDrainingOrGone() {
    List<Offer> offers = List.of(
        new Offer("p1", true, true, A),
        new Offer("p2", true, false, B),
        new Offer("p3", false, false, A),
        new Offer("p0", true, false, null));
    assertNull(BedrockDoors.pick("p2", offers));
    assertEquals(B, BedrockDoors.pick("p9", offers));
    assertNull(BedrockDoors.pick("p2", List.of(new Offer("p2", true, false, B))));
  }

  @Test
  void pickUsesTheLowestLiveId() {
    List<Offer> offers = List.of(
        new Offer("p2", true, false, B),
        new Offer("p1", true, false, A));
    assertEquals(A, BedrockDoors.pick("self", offers));
  }
}
