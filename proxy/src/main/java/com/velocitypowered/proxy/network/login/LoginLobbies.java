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

import java.util.List;

/**
 * Main lobbies a player may be moved to once the account is accepted.
 */
public final class LoginLobbies {

  private LoginLobbies() {
  }

  /**
   * A player already standing on one of these lobbies stays there.
   *
   * @param here the server they are on, or null
   * @param lobbies configured main lobbies
   * @return whether they are already on one
   */
  public static boolean staying(String here, List<String> lobbies) {
    if (here == null || lobbies == null) {
      return false;
    }
    for (String lobby : lobbies) {
      if (here.equalsIgnoreCase(lobby)) {
        return true;
      }
    }
    return false;
  }
}
