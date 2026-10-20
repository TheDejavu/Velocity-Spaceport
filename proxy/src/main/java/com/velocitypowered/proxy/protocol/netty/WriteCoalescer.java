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

import com.velocitypowered.proxy.util.VelocityProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.internal.PlatformDependent;
import java.util.ArrayList;
import java.util.List;

/**
 * Spaceport: collects the framed packets written between two flushes into one buffer, so a flush
 * hands the socket (and the cipher) a single buffer instead of one buffer, outbound entry and
 * iovec per packet. Owned by one encoder of one channel; only used on its event loop.
 */
final class WriteCoalescer {

  static final boolean ENABLED =
      VelocityProperties.readBoolean("spaceport.write-coalescing", true);
  // Without Unsafe, Netty 4.2 still exposes direct buffer addresses but cannot copy between them.
  private static final boolean RAW_COPY = PlatformDependent.hasUnsafe();
  private static final int INITIAL_CAPACITY = 8 * 1024;
  // Above this the batch goes to the outbound buffer early, so one burst can't grow it unbounded.
  private static final int MAX_BATCH = 256 * 1024;

  private final boolean direct;
  private ByteBuf batch;
  private List<ChannelPromise> promises;

  WriteCoalescer(boolean direct) {
    this.direct = direct;
  }

  /** Returns the batch buffer with room for at least {@code bytes} more bytes. */
  ByteBuf batch(ChannelHandlerContext ctx, int bytes) {
    if (batch == null) {
      int capacity = Math.max(INITIAL_CAPACITY, bytes);
      batch = direct ? ctx.alloc().directBuffer(capacity) : ctx.alloc().heapBuffer(capacity);
    } else {
      batch.ensureWritable(bytes);
    }
    return batch;
  }

  /** Records the promise of a message now encoded into the batch; writes early if it got big. */
  void added(ChannelHandlerContext ctx, ChannelPromise promise) {
    if (!promise.isVoid()) {
      if (promises == null) {
        promises = new ArrayList<>(2);
      }
      promises.add(promise);
    }
    if (batch.readableBytes() >= MAX_BATCH) {
      writeBatch(ctx);
    }
  }

  /** Passes the batch on to the next handler (without flushing). */
  void writeBatch(ChannelHandlerContext ctx) {
    ByteBuf buf = batch;
    if (buf == null) {
      return;
    }
    batch = null;
    List<ChannelPromise> waiting = promises;
    promises = null;
    if (!buf.isReadable()) {
      buf.release();
      if (waiting != null) {
        waiting.forEach(ChannelPromise::trySuccess);
      }
      return;
    }
    if (waiting == null) {
      ctx.write(buf, ctx.voidPromise());
    } else if (waiting.size() == 1) {
      ctx.write(buf, waiting.get(0));
    } else {
      ctx.write(buf).addListener(f -> {
        for (ChannelPromise p : waiting) {
          if (f.isSuccess()) {
            p.trySuccess();
          } else {
            p.tryFailure(f.cause());
          }
        }
      });
    }
  }

  /**
   * Appends {@code src}'s readable bytes to {@code dst} without moving {@code src}'s reader index.
   * Netty's own direct-to-direct copy creates two NIO ByteBuffer views per call; with raw
   * addresses (Unsafe available) this is a plain memory copy.
   */
  static void append(ByteBuf src, ByteBuf dst) {
    int length = src.readableBytes();
    if (RAW_COPY && src.hasMemoryAddress() && dst.hasMemoryAddress()) {
      dst.ensureWritable(length);
      int writerIndex = dst.writerIndex();
      PlatformDependent.copyMemory(src.memoryAddress() + src.readerIndex(),
          dst.memoryAddress() + writerIndex, length);
      dst.writerIndex(writerIndex + length);
    } else {
      dst.writeBytes(src, src.readerIndex(), length);
    }
  }

  /** Drops the batch (channel gone); fails any waiting promises. */
  void discard(Throwable cause) {
    if (batch != null) {
      batch.release();
      batch = null;
    }
    if (promises != null) {
      promises.forEach(p -> p.tryFailure(cause));
      promises = null;
    }
  }
}
