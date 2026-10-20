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

import static com.velocitypowered.proxy.network.Connections.CIPHER_DECODER;
import static com.velocitypowered.proxy.network.Connections.CIPHER_ENCODER;
import static com.velocitypowered.proxy.network.Connections.COMPRESSION_DECODER;
import static com.velocitypowered.proxy.network.Connections.COMPRESSION_ENCODER;
import static com.velocitypowered.proxy.network.Connections.FLOW_HANDLER;
import static com.velocitypowered.proxy.network.Connections.FRAME_DECODER;
import static com.velocitypowered.proxy.network.Connections.FRAME_ENCODER;
import static com.velocitypowered.proxy.network.Connections.HANDLER;
import static com.velocitypowered.proxy.network.Connections.LEGACY_PING_DECODER;
import static com.velocitypowered.proxy.network.Connections.LEGACY_PING_ENCODER;
import static com.velocitypowered.proxy.network.Connections.MINECRAFT_DECODER;
import static com.velocitypowered.proxy.network.Connections.MINECRAFT_ENCODER;
import static com.velocitypowered.proxy.network.Connections.PLAY_PACKET_QUEUE_INBOUND;
import static com.velocitypowered.proxy.network.Connections.PLAY_PACKET_QUEUE_OUTBOUND;
import static com.velocitypowered.proxy.network.Connections.READ_TIMEOUT;

import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.BackendPlaySessionHandler;
import com.velocitypowered.proxy.protocol.ProtocolUtils;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.util.VelocityProperties;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import java.util.List;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Spaceport: forwards play packets that Velocity does not handle straight from the backend
 * connection to the player's encoders, skipping the decoder, session-handler and outbound
 * handler hops. When both sides use the same compression threshold, compressed packets are
 * forwarded still compressed (only the packet id is inflated to look it up), so they are neither
 * inflated nor deflated again.
 *
 * <p>Only used while both pipelines contain nothing but Velocity's own handlers; if a plugin
 * (ViaVersion, packetevents, ...) adds a handler to either connection, every packet takes the
 * normal path.
 */
public final class PassthroughHandler extends ChannelInboundHandlerAdapter {

  public static final String NAME = "spaceport-passthrough";
  public static final boolean ENABLED =
      VelocityProperties.readBoolean("spaceport.passthrough-fast-path", true);
  private static final boolean COMPRESSED =
      VelocityProperties.readBoolean("spaceport.compressed-passthrough", true);
  private static final int LARGE_PACKET = 128 * 1024;
  private static final int VANILLA_MAX_UNCOMPRESSED = 8 * 1024 * 1024;
  private static final long RECHECK_NANOS = 1_000_000_000L;
  // compressed bytes tried first; a dynamic Huffman header plus the id usually fit
  private static final int PEEK_INPUT = 256;

  private static final Set<String> BACKEND_HANDLERS = Set.of(FRAME_DECODER, READ_TIMEOUT, NAME,
      COMPRESSION_DECODER, MINECRAFT_DECODER, FLOW_HANDLER, MINECRAFT_ENCODER, COMPRESSION_ENCODER,
      FRAME_ENCODER, HANDLER, PLAY_PACKET_QUEUE_INBOUND, PLAY_PACKET_QUEUE_OUTBOUND);
  private static final Set<String> CLIENT_HANDLERS = Set.of(LEGACY_PING_DECODER,
      LEGACY_PING_ENCODER, FRAME_DECODER, READ_TIMEOUT, CIPHER_DECODER, CIPHER_ENCODER,
      COMPRESSION_DECODER, COMPRESSION_ENCODER, FRAME_ENCODER, MINECRAFT_DECODER,
      MINECRAFT_ENCODER, HANDLER, PLAY_PACKET_QUEUE_INBOUND, PLAY_PACKET_QUEUE_OUTBOUND);

  private final MinecraftConnection backend;
  private final MinecraftConnection client;
  private final Inflater inflater = new Inflater();
  private final byte[] idBytes = new byte[5];
  private final byte[] peekInput = new byte[4 * PEEK_INPUT];

  // refreshed by check()
  private long checkedAt;
  private boolean checked;
  private boolean usable;
  private boolean backendCompressed;
  private boolean forwardCompressed;
  private ChannelHandlerContext clientEncoder;
  private AutoReadHolderHandler flowHandler;
  private MinecraftDecoder minecraftDecoder;

  private PassthroughHandler(MinecraftConnection backend, MinecraftConnection client) {
    this.backend = backend;
    this.client = client;
  }

  /**
   * Adds the fast path to a backend connection entering play state, if enabled.
   *
   * @param backend the backend connection
   * @param client the player's connection
   */
  public static void install(MinecraftConnection backend, MinecraftConnection client) {
    if (!ENABLED) {
      return;
    }
    ChannelPipeline pipeline = backend.getChannel().pipeline();
    if (pipeline.get(NAME) != null || pipeline.get(MINECRAFT_DECODER) == null) {
      return;
    }
    String before = pipeline.get(COMPRESSION_DECODER) != null
        ? COMPRESSION_DECODER : MINECRAFT_DECODER;
    pipeline.addBefore(before, NAME, new PassthroughHandler(backend, client));
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (!(msg instanceof ByteBuf buf) || !ready() || !forward(buf)) {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) {
    inflater.end();
  }

  /** Per-packet conditions; the expensive pipeline check runs at most once a second. */
  private boolean ready() {
    if (backend.getState() != StateRegistry.PLAY || client.getState() != StateRegistry.PLAY
        || !(backend.getActiveSessionHandler() instanceof BackendPlaySessionHandler)
        || !backend.getChannel().config().isAutoRead() || client.isClosed()) {
      return false;
    }
    long now = System.nanoTime();
    if (!checked || now - checkedAt > RECHECK_NANOS) {
      checked = true;
      checkedAt = now;
      check();
    }
    return usable && !flowHandler.hasQueued();
  }

  private void check() {
    usable = false;
    Channel clientChannel = client.getChannel();
    ChannelPipeline bp = backend.getChannel().pipeline();
    ChannelPipeline cp = clientChannel.pipeline();
    if (!clientChannel.eventLoop().inEventLoop() || !onlyKnown(bp.names(), BACKEND_HANDLERS)
        || !onlyKnown(cp.names(), CLIENT_HANDLERS)) {
      return;
    }
    // we sit right in front of the compression decoder, or of the minecraft decoder
    List<String> names = bp.names();
    int self = names.indexOf(NAME);
    String next = self >= 0 && self + 1 < names.size() ? names.get(self + 1) : null;
    MinecraftCompressDecoder decoder = (MinecraftCompressDecoder) bp.get(COMPRESSION_DECODER);
    if (decoder != null ? !COMPRESSION_DECODER.equals(next) : !MINECRAFT_DECODER.equals(next)) {
      return;
    }
    if (!(bp.get(FLOW_HANDLER) instanceof AutoReadHolderHandler flow)) {
      return;
    }
    final MinecraftCompressorAndLengthEncoder encoder =
        (MinecraftCompressorAndLengthEncoder) cp.get(COMPRESSION_ENCODER);
    clientEncoder = cp.context(MINECRAFT_ENCODER);
    if (clientEncoder == null) {
      return;
    }
    if (!(bp.get(MINECRAFT_DECODER) instanceof MinecraftDecoder mcDecoder)) {
      return;
    }
    minecraftDecoder = mcDecoder;
    flowHandler = flow;
    backendCompressed = decoder != null;
    forwardCompressed = COMPRESSED && decoder != null && encoder != null
        && decoder.getThreshold() == encoder.getThreshold();
    usable = true;
  }

  private static boolean onlyKnown(List<String> names, Set<String> known) {
    for (String name : names) {
      if (!known.contains(name) && !name.startsWith("DefaultChannelPipeline$TailContext")) {
        return false;
      }
    }
    return true;
  }

  /** Returns true if the packet was taken over (and ownership with it). */
  private boolean forward(ByteBuf frame) throws DataFormatException {
    int start = frame.readerIndex();
    int packetId;
    int dataLength = 0;
    if (backendCompressed) {
      dataLength = ProtocolUtils.readVarInt(frame);
      if (dataLength == 0) {
        packetId = ProtocolUtils.readVarInt(frame);
      } else if (!forwardCompressed || dataLength > VANILLA_MAX_UNCOMPRESSED) {
        frame.readerIndex(start);
        return false;
      } else {
        packetId = peekCompressedId(inflater, peekInput, idBytes, frame);
      }
    } else {
      packetId = ProtocolUtils.readVarInt(frame);
    }
    if (packetId < 0 || minecraftDecoder.isRegistered(packetId)) {
      frame.readerIndex(start); // Velocity handles this one
      return false;
    }
    BackendPlaySessionHandler handler = (BackendPlaySessionHandler) backend
        .getActiveSessionHandler();
    if (handler.beforeHandle()) { // obsolete connection: drop, as the normal path does
      frame.release();
      return true;
    }
    int size;
    if (forwardCompressed) {
      frame.readerIndex(start);
      size = Math.max(dataLength, frame.readableBytes());
      clientEncoder.write(new PrecompressedFrame(frame), clientEncoder.voidPromise());
    } else {
      // uncompressed from the backend: hand the packet (id + body) to the player's encoder
      frame.readerIndex(backendCompressed ? start + ProtocolUtils.varIntBytes(0) : start);
      size = frame.readableBytes();
      clientEncoder.write(frame, clientEncoder.voidPromise());
    }
    handler.passthroughWritten(size > LARGE_PACKET);
    return true;
  }

  /**
   * Inflates just enough of a compressed packet to read its id; -1 if that fails. Feeds the
   * inflater a growing prefix copied into {@code scratch}, so no buffer views are allocated.
   */
  static int peekCompressedId(Inflater inflater, byte[] scratch, byte[] idBytes, ByteBuf frame)
      throws DataFormatException {
    int available = frame.readableBytes();
    for (int length = Math.min(PEEK_INPUT, available); ; length = Math.min(length * 4, available)) {
      byte[] input = length <= scratch.length ? scratch : new byte[length];
      frame.getBytes(frame.readerIndex(), input, 0, length);
      inflater.reset();
      inflater.setInput(input, 0, length);
      int n = inflater.inflate(idBytes);
      int value = 0;
      for (int i = 0; i < n; i++) {
        value |= (idBytes[i] & 0x7F) << (i * 7);
        if ((idBytes[i] & 0x80) == 0) {
          return value;
        }
      }
      if (length == available || n == idBytes.length) {
        return -1;
      }
    }
  }
}
