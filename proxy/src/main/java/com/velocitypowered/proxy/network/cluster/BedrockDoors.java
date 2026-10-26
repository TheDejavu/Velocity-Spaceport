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

import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The Geysers the network is willing to send a Bedrock player to.
 */
public final class BedrockDoors {

  private BedrockDoors() {
  }

  /**
   * One Geyser Bedrock clients can connect to.
   *
   * @param host the address
   * @param port the port
   */
  public record Door(String host, int port) {
  }

  /**
   * A proxy's Geyser, as the last tick saw that proxy.
   *
   * @param id the proxy id
   * @param alive whether the proxy is still in the network
   * @param draining whether the proxy is draining
   * @param door its Geyser, or null if it has none
   */
  public record Offer(String id, boolean alive, boolean draining, @Nullable Door door) {
  }

  /**
   * Reads {@code host:port}.
   *
   * @param text the stored value
   * @return the door, or null if it is not a host and a port
   */
  public static @Nullable Door parse(@Nullable String text) {
    if (text == null) {
      return null;
    }
    int colon = text.lastIndexOf(':');
    if (colon <= 0 || colon == text.length() - 1) {
      return null;
    }
    int port;
    try {
      port = Integer.parseInt(text.substring(colon + 1));
    } catch (NumberFormatException bad) {
      return null;
    }
    String host = text.substring(0, colon);
    if (host.isBlank() || port < 1 || port > 65535) {
      return null;
    }
    return new Door(host, port);
  }

  /**
   * The live Geyser to send players to: the lowest proxy id that is up, not draining, and not
   * this proxy.
   *
   * @param self this proxy's id
   * @param offers every proxy
   * @return the door, or null if there is nowhere to send them
   */
  public static @Nullable Door pick(String self, List<Offer> offers) {
    Offer chosen = null;
    for (Offer offer : offers) {
      if (offer.id().equals(self) || !offer.alive() || offer.draining() || offer.door() == null) {
        continue;
      }
      if (chosen == null || offer.id().compareTo(chosen.id()) < 0) {
        chosen = offer;
      }
    }
    return chosen == null ? null : chosen.door();
  }
}
