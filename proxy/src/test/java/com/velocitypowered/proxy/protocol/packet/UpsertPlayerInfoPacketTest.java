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

package com.velocitypowered.proxy.protocol.packet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.protocol.ProtocolUtils.Direction;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Spaceport: a decoded player info packet is written back from its received bytes, and those
 * bytes are dropped (and released) as soon as the packet is changed.
 */
class UpsertPlayerInfoPacketTest {

  private static final ProtocolVersion VERSION = ProtocolVersion.MAXIMUM_VERSION;

  private static ByteBuf encoded() {
    UUID id = UUID.randomUUID();
    UpsertPlayerInfoPacket.Entry entry = new UpsertPlayerInfoPacket.Entry(id);
    entry.setProfile(new GameProfile(id, "Steve",
        List.of(new GameProfile.Property("textures", "skin", "signature"))));
    entry.setListed(true);
    entry.setLatency(42);
    UpsertPlayerInfoPacket packet = new UpsertPlayerInfoPacket(
        EnumSet.of(UpsertPlayerInfoPacket.Action.ADD_PLAYER,
            UpsertPlayerInfoPacket.Action.UPDATE_LISTED,
            UpsertPlayerInfoPacket.Action.UPDATE_LATENCY), List.of(entry));
    ByteBuf buf = Unpooled.buffer();
    packet.encode(buf, Direction.CLIENTBOUND, VERSION);
    return buf;
  }

  @Test
  void writesBackTheReceivedBytes() {
    ByteBuf wire = encoded();
    UpsertPlayerInfoPacket decoded = new UpsertPlayerInfoPacket();
    decoded.decode(wire.duplicate(), Direction.CLIENTBOUND, VERSION);
    assertEquals("Steve", decoded.getEntries().get(0).getProfile().getName());

    ByteBuf out = Unpooled.buffer();
    decoded.encode(out, Direction.CLIENTBOUND, VERSION);
    assertTrue(ByteBufUtil.equals(wire, out));
    assertEquals(2, wire.refCnt(), "the packet holds a reference to the frame");
    assertFalse(decoded.release());
    assertEquals(1, wire.refCnt());
  }

  @Test
  void changingThePacketDropsTheBytes() {
    ByteBuf wire = encoded();
    UpsertPlayerInfoPacket decoded = new UpsertPlayerInfoPacket();
    decoded.decode(wire.duplicate(), Direction.CLIENTBOUND, VERSION);
    decoded.addAction(UpsertPlayerInfoPacket.Action.UPDATE_GAME_MODE);
    assertEquals(1, wire.refCnt(), "kept bytes released on change");

    ByteBuf out = Unpooled.buffer();
    decoded.encode(out, Direction.CLIENTBOUND, VERSION);
    assertFalse(ByteBufUtil.equals(wire, out), "re-encoded with the new action");
  }
}
