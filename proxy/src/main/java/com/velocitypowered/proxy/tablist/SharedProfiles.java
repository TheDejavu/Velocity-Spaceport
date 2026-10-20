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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.util.VelocityProperties;
import java.util.List;
import java.util.UUID;

/**
 * Spaceport: every player's tab list keeps a profile (with its skin properties) for every other
 * player it lists, so a big server holds the same profile once per viewer. Equal profiles decoded
 * from player info packets are shared instead; unused ones are dropped by the garbage collector.
 */
public final class SharedProfiles {

  private static final boolean ENABLED =
      VelocityProperties.readBoolean("spaceport.share-tab-profiles", true);
  private static final Cache<Key, GameProfile> PROFILES =
      Caffeine.newBuilder().weakValues().build();

  private SharedProfiles() {
  }

  private record Prop(String name, String value, String signature) {
  }

  private record Key(UUID id, String name, List<Prop> properties) {
  }

  /**
   * Returns a shared instance equal to {@code profile}.
   *
   * @param profile a freshly decoded profile
   * @return an equal profile, possibly shared with other tab lists
   */
  public static GameProfile share(GameProfile profile) {
    if (!ENABLED) {
      return profile;
    }
    List<Prop> props = profile.getProperties().stream()
        .map(p -> new Prop(p.getName(), p.getValue(), p.getSignature()))
        .toList();
    return PROFILES.get(new Key(profile.getId(), profile.getName(), props), k -> profile);
  }
}
