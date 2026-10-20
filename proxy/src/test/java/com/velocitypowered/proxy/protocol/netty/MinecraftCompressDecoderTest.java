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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import java.util.Arrays;
import java.util.zip.Deflater;
import org.junit.jupiter.api.Test;

/**
 * Spaceport: compressed frames claiming more than deflate can produce are rejected up front.
 */
class MinecraftCompressDecoderTest {

  private static EmbeddedChannel channel() {
    return new EmbeddedChannel(new MinecraftCompressDecoder(256,
        Natives.compress.get().create(6), ProtocolUtils.Direction.SERVERBOUND));
  }

  private static ByteBuf frame(int claimed, byte[] compressed) {
    ByteBuf buf = Unpooled.buffer();
    ProtocolUtils.writeVarInt(buf, claimed);
    buf.writeBytes(compressed);
    return buf;
  }

  @Test
  void rejectsImpossibleClaim() {
    EmbeddedChannel ch = channel();
    byte[] junk = new byte[100];
    assertThrows(DecoderException.class, () -> ch.writeInbound(frame(2 * 1024 * 1024, junk)));
    ch.finishAndReleaseAll();
  }

  @Test
  void acceptsBestCaseDeflate() {
    byte[] zeros = new byte[2 * 1024 * 1024];
    Deflater deflater = new Deflater(9);
    deflater.setInput(zeros);
    deflater.finish();
    byte[] out = new byte[64 * 1024];
    int len = deflater.deflate(out);
    deflater.end();

    EmbeddedChannel ch = channel();
    ch.writeInbound(frame(zeros.length, Arrays.copyOf(out, len)));
    ByteBuf got = ch.readInbound();
    assertEquals(zeros.length, got.readableBytes());
    got.release();
    ch.finishAndReleaseAll();
  }
}
