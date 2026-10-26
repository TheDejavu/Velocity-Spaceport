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

import com.velocitypowered.api.util.GameProfile;
import java.util.ArrayList;
import java.util.List;

/**
 * A profile property that only exists after Mojang's session server accepted the client. Modern
 * forwarding signs the property list, so a backend can trust it and a client cannot add it.
 */
public final class PremiumProof {

  public static final String PROPERTY = "spaceport:premium";

  private static final String PRESENT = "1";

  private PremiumProof() {
  }

  public static GameProfile stamp(GameProfile profile) {
    if (carries(profile)) {
      return profile;
    }
    List<GameProfile.Property> properties = new ArrayList<>(profile.getProperties());
    properties.add(new GameProfile.Property(PROPERTY, PRESENT, ""));
    return profile.withProperties(properties);
  }

  public static boolean carries(GameProfile profile) {
    for (GameProfile.Property property : profile.getProperties()) {
      if (PROPERTY.equals(property.getName()) && PRESENT.equals(property.getValue())) {
        return true;
      }
    }
    return false;
  }
}
