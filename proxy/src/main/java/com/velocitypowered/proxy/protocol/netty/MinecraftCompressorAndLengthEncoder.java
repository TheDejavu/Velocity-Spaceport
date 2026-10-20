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

import static com.velocitypowered.proxy.protocol.netty.MinecraftVarintLengthEncoder.IS_JAVA_CIPHER;

import com.velocitypowered.natives.compression.VelocityCompressor;
import com.velocitypowered.natives.util.BufferPreference;
import com.velocitypowered.natives.util.MoreByteBufUtils;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import java.nio.channels.ClosedChannelException;
import java.util.zip.DataFormatException;

/**
 * Handler for compressing Minecraft packets.
 */
public class MinecraftCompressorAndLengthEncoder extends MessageToByteEncoder<ByteBuf> {

  private int threshold;
  private final VelocityCompressor compressor;
  private final WriteCoalescer coalescer;

  /**
   * Creates the encoder.
   *
   * @param threshold the compression threshold
   * @param compressor the compressor to use
   */
  public MinecraftCompressorAndLengthEncoder(int threshold, VelocityCompressor compressor) {
    this.threshold = threshold;
    this.compressor = compressor;
    this.coalescer = WriteCoalescer.ENABLED ? new WriteCoalescer(!IS_JAVA_CIPHER
        || compressor.preferredBufferType() == BufferPreference.DIRECT_REQUIRED) : null;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof PrecompressedFrame frame) {
      writePrecompressed(ctx, frame, promise);
      return;
    }
    if (coalescer == null || !(msg instanceof ByteBuf buf)) {
      if (coalescer != null) {
        coalescer.writeBatch(ctx); // keep order with whatever passes us by
      }
      super.write(ctx, msg, promise);
      return;
    }
    // worst case: 3-byte length + 5-byte size + data that did not compress
    ByteBuf out = coalescer.batch(ctx, buf.readableBytes() + 8);
    int mark = out.writerIndex();
    try {
      encode(ctx, buf, out);
    } catch (Throwable e) {
      out.writerIndex(mark); // drop the half-written packet, keep the rest of the batch
      throw e instanceof EncoderException ee ? ee : new EncoderException(e);
    } finally {
      ReferenceCountUtil.release(buf);
    }
    coalescer.added(ctx, promise);
  }

  private void writePrecompressed(ChannelHandlerContext ctx, PrecompressedFrame frame,
      ChannelPromise promise) {
    try {
      ByteBuf data = frame.content();
      int length = data.readableBytes();
      if (coalescer != null) {
        ByteBuf out = coalescer.batch(ctx, length + 3);
        ProtocolUtils.writeVarInt(out, length);
        WriteCoalescer.append(data, out);
        coalescer.added(ctx, promise);
      } else {
        ByteBuf out = IS_JAVA_CIPHER ? ctx.alloc().heapBuffer(length + 3)
            : ctx.alloc().directBuffer(length + 3);
        ProtocolUtils.writeVarInt(out, length);
        out.writeBytes(data, data.readerIndex(), length);
        ctx.write(out, promise);
      }
    } finally {
      frame.release();
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    if (coalescer != null) {
      coalescer.writeBatch(ctx);
    }
    ctx.flush();
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out) throws Exception {
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      // Under the threshold, there is nothing to do.
      ProtocolUtils.writeVarInt(out, uncompressed + 1);
      out.writeByte(0);
      WriteCoalescer.append(msg, out);
      msg.skipBytes(uncompressed);
    } else {
      handleCompressed(ctx, msg, out);
    }
  }

  private void handleCompressed(ChannelHandlerContext ctx, ByteBuf msg, ByteBuf out)
      throws DataFormatException {
    int uncompressed = msg.readableBytes();

    final int start = out.writerIndex();
    out.writeMedium(0); // Reserve the packet length
    ProtocolUtils.writeVarInt(out, uncompressed);
    ByteBuf compatibleIn = MoreByteBufUtils.ensureCompatible(ctx.alloc(), compressor, msg);

    int startCompressed = out.writerIndex();
    try {
      compressor.deflate(compatibleIn, out);
    } finally {
      compatibleIn.release();
    }
    int compressedLength = out.writerIndex() - startCompressed;
    if (compressedLength >= 1 << 21) {
      throw new DataFormatException("The server sent a very large (over 2MiB compressed) packet.");
    }

    int packetLength = out.writerIndex() - start - 3;
    out.setMedium(start, ProtocolUtils.encode21BitVarInt(packetLength)); // Rewrite packet length
  }

  @Override
  protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, ByteBuf msg, boolean preferDirect)
      throws Exception {
    int uncompressed = msg.readableBytes();
    if (uncompressed < threshold) {
      int finalBufferSize = uncompressed + 1;
      finalBufferSize += ProtocolUtils.varIntBytes(finalBufferSize);
      return IS_JAVA_CIPHER
          ? ctx.alloc().heapBuffer(finalBufferSize)
          : ctx.alloc().directBuffer(finalBufferSize);
    }

    // (maximum data length after compression) + packet length varint + uncompressed data varint
    int initialBufferSize = (uncompressed - 1) + 3 + ProtocolUtils.varIntBytes(uncompressed);
    return MoreByteBufUtils.preferredBuffer(ctx.alloc(), compressor, initialBufferSize);
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    if (coalescer != null) {
      if (ctx.channel().isActive()) {
        coalescer.writeBatch(ctx); // replaced (e.g. threshold change): keep what we hold
      } else {
        coalescer.discard(new ClosedChannelException());
      }
    }
    compressor.close();
  }

  public int getThreshold() {
    return threshold;
  }

  public void setThreshold(int threshold) {
    this.threshold = threshold;
  }
}
