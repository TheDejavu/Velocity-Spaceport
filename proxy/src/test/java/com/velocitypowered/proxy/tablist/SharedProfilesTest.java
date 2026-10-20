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

package com.velocitypowered.proxy.tablist;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.velocitypowered.api.util.GameProfile;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Spaceport: equal tab-list profiles are shared, different ones are not.
 */
class SharedProfilesTest {

  private static GameProfile profile(UUID id, String skin) {
    return new GameProfile(id, "Steve",
        List.of(new GameProfile.Property("textures", new String(skin.toCharArray()), "sig")));
  }

  @Test
  void sharesEqualProfiles() {
    UUID id = UUID.randomUUID();
    GameProfile first = SharedProfiles.share(profile(id, "skin-a"));
    assertSame(first, SharedProfiles.share(profile(id, "skin-a")));
    assertNotSame(first, SharedProfiles.share(profile(id, "skin-b")), "skin changed");
    assertNotSame(first, SharedProfiles.share(profile(UUID.randomUUID(), "skin-a")), "other uuid");
  }
}
