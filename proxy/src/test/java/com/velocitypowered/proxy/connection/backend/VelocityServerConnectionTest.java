/*
 * Copyright (C) 2018-2021 Velocity Contributors
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

package com.velocitypowered.proxy.connection.backend;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.api.proxy.ConnectionRequestBuilder;
import com.velocitypowered.api.proxy.server.ServerInfo;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.connection.util.ConnectionRequestResults.Impl;
import com.velocitypowered.proxy.server.VelocityRegisteredServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class VelocityServerConnectionTest {

  /**
   * A connection that is torn down while its TCP connect is still pending must not log in to the
   * backend afterwards (it used to, leaving a ghost session for a player that had gone).
   */
  @Test
  void connectFinishingAfterTeardownIsClosed() throws Exception {
    NioEventLoopGroup group = new NioEventLoopGroup(1);
    try (ServerSocket backend = new ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
      EventLoop loop = group.next();
      VelocityServer server = mock(VelocityServer.class);
      when(server.createBootstrap(loop)).thenReturn(new Bootstrap().group(loop)
          .channel(NioSocketChannel.class));
      when(server.getBackendChannelInitializer()).thenReturn(new ChannelInitializer<>() {
        @Override
        protected void initChannel(io.netty.channel.Channel ch) {
        }
      });
      MinecraftConnection playerConnection = mock(MinecraftConnection.class);
      when(playerConnection.eventLoop()).thenReturn(loop);
      ConnectedPlayer player = mock(ConnectedPlayer.class);
      when(player.getConnection()).thenReturn(playerConnection);
      when(player.isActive()).thenReturn(true);
      VelocityRegisteredServer target = mock(VelocityRegisteredServer.class);
      when(target.getServerInfo()).thenReturn(new ServerInfo("b",
          new InetSocketAddress(backend.getInetAddress(), backend.getLocalPort())));

      VelocityServerConnection con = new VelocityServerConnection(target, null, player, server);

      // Hold the event loop so the connect cannot complete before the teardown below.
      CountDownLatch release = new CountDownLatch(1);
      loop.execute(() -> {
        try {
          release.await();
        } catch (InterruptedException ignored) {
          Thread.currentThread().interrupt();
        }
      });
      var result = con.connect();
      con.disconnect();
      release.countDown();

      Impl impl = result.get(10, TimeUnit.SECONDS);
      assertEquals(ConnectionRequestBuilder.Status.CONNECTION_CANCELLED, impl.getStatus());
      assertFalse(impl.isSuccessful());

      backend.setSoTimeout(5000);
      try (Socket accepted = backend.accept()) {
        accepted.setSoTimeout(5000);
        // Nothing may be sent (no handshake), the proxy side just closes.
        assertEquals(-1, accepted.getInputStream().read());
      }
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }
}
