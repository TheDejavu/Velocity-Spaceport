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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class LoginLobbiesTest {

  @Test
  void aPlayerOnAMainLobbyStaysThere() {
    List<String> lobbies = List.of("lobby-1", "lobby-2");
    assertTrue(LoginLobbies.staying("lobby-2", lobbies));
    assertTrue(LoginLobbies.staying("LOBBY-1", lobbies));
    assertFalse(LoginLobbies.staying("auth-1", lobbies));
    assertFalse(LoginLobbies.staying(null, lobbies));
    assertFalse(LoginLobbies.staying("lobby-1", List.of()));
  }
}
