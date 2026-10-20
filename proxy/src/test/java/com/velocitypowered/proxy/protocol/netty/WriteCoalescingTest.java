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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Spaceport write coalescing: packets written between flushes leave as one buffer, byte-identical
 * to the frames the uncoalesced encoders produce.
 */
class WriteCoalescingTest {

  private static ByteBuf packet(int size, long seed) {
    byte[] data = new byte[size];
    new Random(seed).nextBytes(data);
    for (int i = 0; i < size / 2; i++) {
      data[i] = 7; // compressible half
    }
    return Unpooled.wrappedBuffer(data);
  }

  /** Reads frames back through the real decoders. */
  private static List<ByteBuf> decode(ByteBuf wire, int threshold) {
    EmbeddedChannel in = new EmbeddedChannel(
        new MinecraftVarintFrameDecoder(ProtocolUtils.Direction.CLIENTBOUND));
    if (threshold >= 0) {
      in.pipeline().addLast(new MinecraftCompressDecoder(threshold,
          Natives.compress.get().create(6), ProtocolUtils.Direction.CLIENTBOUND));
    }
    in.writeInbound(wire);
    List<ByteBuf> out = new ArrayList<>();
    ByteBuf b;
    while ((b = in.readInbound()) != null) {
      out.add(b);
    }
    in.finishAndReleaseAll();
    return out;
  }

  private static void assertRoundTrip(EmbeddedChannel ch, int threshold, int... sizes) {
    List<ByteBuf> sent = new ArrayList<>();
    for (int i = 0; i < sizes.length; i++) {
      ByteBuf p = packet(sizes[i], i);
      sent.add(p.copy());
      ch.write(p);
    }
    assertNull(ch.readOutbound(), "nothing may leave before the flush");
    ch.flush();
    ByteBuf wire = ch.readOutbound();
    assertNull(ch.readOutbound(), "one flush, one buffer");
    List<ByteBuf> got = decode(wire, threshold);
    assertEquals(sizes.length, got.size());
    for (int i = 0; i < sizes.length; i++) {
      assertTrue(ByteBufUtil.equals(sent.get(i), got.get(i)), "packet " + i);
      sent.get(i).release();
      got.get(i).release();
    }
  }

  @Test
  void lengthEncoderBatchesUntilFlush() {
    EmbeddedChannel ch = new EmbeddedChannel(MinecraftVarintLengthEncoder.create());
    assertRoundTrip(ch, -1, 1, 20, 300, 70000);
    assertRoundTrip(ch, -1, 5);
    assertFalse(ch.finishAndReleaseAll());
  }

  @Test
  void compressionEncoderBatchesUntilFlush() {
    EmbeddedChannel ch = new EmbeddedChannel(
        new MinecraftCompressorAndLengthEncoder(256, Natives.compress.get().create(6)));
    assertRoundTrip(ch, 256, 10, 255, 256, 5000, 3, 200000);
    assertFalse(ch.finishAndReleaseAll());
  }

  @Test
  void appendCopiesBetweenDirectBuffers() {
    // runs with or without Unsafe (the test JVM on Java 25 has it off by default)
    ByteBuf src = PooledByteBufAllocator.DEFAULT.directBuffer(64);
    ByteBuf dst = PooledByteBufAllocator.DEFAULT.directBuffer(4);
    src.writeBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
    src.readerIndex(2);
    dst.writeByte(9);
    WriteCoalescer.append(src, dst);
    assertEquals(2, src.readerIndex(), "source reader index is left alone");
    assertTrue(ByteBufUtil.equals(Unpooled.wrappedBuffer(new byte[] {9, 3, 4, 5, 6, 7, 8}), dst));
    src.release();
    dst.release();
  }

  @Test
  void promisesCompleteWhenTheBatchIsWritten() {
    EmbeddedChannel ch = new EmbeddedChannel(MinecraftVarintLengthEncoder.create());
    ChannelPromise a = ch.newPromise();
    ChannelPromise b = ch.newPromise();
    ch.write(packet(10, 1), a);
    ch.write(packet(10, 2), b);
    assertFalse(a.isDone());
    ch.flush();
    assertTrue(a.isSuccess());
    assertTrue(b.isSuccess());
    ch.finishAndReleaseAll();
  }

  @Test
  void removingTheEncoderKeepsBufferedPackets() {
    MinecraftVarintLengthEncoder enc = MinecraftVarintLengthEncoder.create();
    EmbeddedChannel ch = new EmbeddedChannel(enc);
    ch.write(packet(10, 1));
    ch.pipeline().remove(enc); // as when compression gets enabled
    ch.flush();
    ByteBuf wire = ch.readOutbound();
    List<ByteBuf> got = decode(wire, -1);
    assertEquals(1, got.size());
    got.forEach(ByteBuf::release);
    ch.finishAndReleaseAll();
  }
}
