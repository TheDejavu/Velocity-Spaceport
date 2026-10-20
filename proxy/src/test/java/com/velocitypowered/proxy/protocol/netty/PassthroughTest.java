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

package com.velocitypowered.proxy.protocol.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Random;
import java.util.zip.Inflater;
import org.junit.jupiter.api.Test;

/**
 * Spaceport passthrough: the id of a compressed packet is read without inflating it all, and a
 * compressed frame forwarded as-is decodes to the original packet on the other side.
 */
class PassthroughTest {

  /** Compresses a packet the way the compression encoder does; returns the frame body. */
  private static ByteBuf compressedBody(ByteBuf packet) {
    EmbeddedChannel enc = new EmbeddedChannel(
        new MinecraftCompressorAndLengthEncoder(256, Natives.compress.get().create(6)));
    enc.writeAndFlush(packet);
    ByteBuf wire = enc.readOutbound();
    ProtocolUtils.readVarInt(wire); // frame length
    enc.finishAndReleaseAll();
    return wire;
  }

  private static ByteBuf packet(int id, int size) {
    ByteBuf buf = Unpooled.buffer();
    ProtocolUtils.writeVarInt(buf, id);
    byte[] data = new byte[size];
    new Random(id).nextBytes(data);
    buf.writeBytes(data);
    return buf;
  }

  @Test
  void peeksIdOfCompressedPacket() throws Exception {
    for (int id : new int[] {0, 5, 127, 128, 300}) {
      ByteBuf body = compressedBody(packet(id, 5000));
      int dataLength = ProtocolUtils.readVarInt(body);
      assertTrue(dataLength > 0);
      int start = body.readerIndex();
      assertEquals(id, PassthroughHandler.peekCompressedId(new Inflater(), new byte[16], new byte[5], body));
      assertEquals(start, body.readerIndex(), "peeking does not consume");
      body.release();
    }
  }

  @Test
  void precompressedFrameDecodesToTheOriginalPacket() {
    ByteBuf original = packet(42, 5000);
    ByteBuf body = compressedBody(original.copy());

    EmbeddedChannel client = new EmbeddedChannel(
        new MinecraftCompressorAndLengthEncoder(256, Natives.compress.get().create(6)));
    client.writeAndFlush(new PrecompressedFrame(body));
    ByteBuf wire = client.readOutbound();

    EmbeddedChannel in = new EmbeddedChannel(
        new MinecraftVarintFrameDecoder(ProtocolUtils.Direction.CLIENTBOUND),
        new MinecraftCompressDecoder(256, Natives.compress.get().create(6),
            ProtocolUtils.Direction.CLIENTBOUND));
    in.writeInbound(wire);
    ByteBuf got = in.readInbound();
    assertTrue(ByteBufUtil.equals(original, got));
    got.release();
    original.release();
    in.finishAndReleaseAll();
    client.finishAndReleaseAll();
  }
}
