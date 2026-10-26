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

package com.velocitypowered.proxy.network.login;

/**
 * Plugin channels a player can see and write. Login decisions do not travel on them.
 */
public final class PlayerLoginChannels {

  private static final String FASTLOGIN = "fastlogin:";
  private static final String SPACEPORT = "spaceport:";

  private PlayerLoginChannels() {
  }

  /** Whether a client may send on this channel. */
  public static boolean fromTheClient(String channel) {
    if (channel == null) {
      return false;
    }
    return channel.regionMatches(true, 0, FASTLOGIN, 0, FASTLOGIN.length())
        || channel.regionMatches(true, 0, SPACEPORT, 0, SPACEPORT.length());
  }
}
