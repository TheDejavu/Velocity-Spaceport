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

import com.velocitypowered.api.network.NetworkMessenger;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.network.cluster.PluginMessenger;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.kyori.adventure.key.Key;

/**
 * Tells the other proxies that a name just passed Mojang's session check. The notice is not a
 * login command. Backends trust the signed profile property, not this packet.
 */
public final class LoginBus {

  public static final Key CHANNEL = Key.key("spaceport", "login");

  private static final Logger logger = Logger.getLogger(LoginBus.class.getName());

  private LoginBus() {
  }

  public static void listen(PluginMessenger messenger) {
    messenger.registerInternal(CHANNEL, (channel, from, payload) -> {
      String name = nameOf(payload);
      if (name != null) {
        logger.info("Premium login shared for " + name);
      }
    });
  }

  public static void tell(VelocityServer server, GameProfile profile) {
    NetworkMessenger messenger = server.getNetwork().getMessenger();
    messenger.broadcast(CHANNEL, payload(profile)).exceptionally(error -> {
      logger.log(Level.WARNING, "Could not share the premium login for " + profile.getName(), error);
      return 0;
    });
  }

  private static String nameOf(byte[] payload) {
    try {
      return new DataInputStream(new ByteArrayInputStream(payload)).readUTF();
    } catch (IOException bad) {
      return null;
    }
  }

  private static byte[] payload(GameProfile profile) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try {
      DataOutputStream out = new DataOutputStream(bytes);
      out.writeUTF(profile.getName());
      out.writeLong(profile.getId().getMostSignificantBits());
      out.writeLong(profile.getId().getLeastSignificantBits());
    } catch (IOException impossible) {
      throw new IllegalStateException(impossible);
    }
    return bytes.toByteArray();
  }
}
