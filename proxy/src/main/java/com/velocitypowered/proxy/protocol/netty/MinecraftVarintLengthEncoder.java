/*
 * Copyright (C) 2018-2023 Velocity Contributors
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

import com.velocitypowered.natives.encryption.JavaVelocityCipher;
import com.velocitypowered.natives.util.Natives;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import java.util.List;

/**
 * Handler for appending a length for Minecraft packets.
 */
@ChannelHandler.Sharable
public class MinecraftVarintLengthEncoder extends MessageToMessageEncoder<ByteBuf> {

  public static final MinecraftVarintLengthEncoder INSTANCE = new MinecraftVarintLengthEncoder();

  static final boolean IS_JAVA_CIPHER = Natives.cipher.get() == JavaVelocityCipher.FACTORY;

  private final WriteCoalescer coalescer;

  private MinecraftVarintLengthEncoder() {
    this.coalescer = null;
  }

  private MinecraftVarintLengthEncoder(WriteCoalescer coalescer) {
    this.coalescer = coalescer;
  }

  /**
   * Returns the encoder for a new pipeline: a per-channel write-coalescing one, or the shared
   * plain {@link #INSTANCE} when Spaceport write coalescing is off.
   *
   * @return the length encoder to add to a pipeline
   */
  public static MinecraftVarintLengthEncoder create() {
    return WriteCoalescer.ENABLED
        ? new MinecraftVarintLengthEncoder(new WriteCoalescer(!IS_JAVA_CIPHER)) : INSTANCE;
  }

  @Override
  public boolean isSharable() {
    return coalescer == null;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (coalescer == null || !(msg instanceof ByteBuf buf)) {
      if (coalescer != null) {
        coalescer.writeBatch(ctx); // keep order with whatever passes us by
      }
      super.write(ctx, msg, promise);
      return;
    }
    try {
      final int length = buf.readableBytes();
      ByteBuf out = coalescer.batch(ctx, length + 3);
      ProtocolUtils.writeVarInt(out, length);
      WriteCoalescer.append(buf, out);
    } finally {
      ReferenceCountUtil.release(buf);
    }
    coalescer.added(ctx, promise);
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    if (coalescer != null) {
      coalescer.writeBatch(ctx);
    }
    ctx.flush();
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (coalescer != null) {
      if (ctx.channel().isActive()) {
        coalescer.writeBatch(ctx); // replaced (compression enabled): keep what we hold
      } else {
        coalescer.discard(new ClosedChannelException());
      }
    }
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf buf,
      List<Object> list) throws Exception {
    final int length = buf.readableBytes();
    final int varintLength = ProtocolUtils.varIntBytes(length);

    final ByteBuf lenBuf = IS_JAVA_CIPHER
        ? ctx.alloc().heapBuffer(varintLength)
        : ctx.alloc().directBuffer(varintLength);

    ProtocolUtils.writeVarInt(lenBuf, length);
    list.add(lenBuf);
    list.add(buf.retain());
  }
}
