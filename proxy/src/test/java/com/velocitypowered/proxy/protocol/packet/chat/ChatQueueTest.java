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

package com.velocitypowered.proxy.protocol.packet.chat;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Spaceport: a chat or command packet that arrives while the player has no live backend
 * connection (kicked, switching) is dropped instead of throwing and kicking the player.
 */
class ChatQueueTest {

  @Test
  void dropsPacketWhenBackendConnectionIsGone() {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    VelocityServerConnection server = mock(VelocityServerConnection.class);
    when(player.getConnectedServer()).thenReturn(server);
    when(server.getConnection()).thenReturn(null); // backend kicked us, fallback not done yet

    ChatQueue queue = new ChatQueue(player);
    AtomicBoolean built = new AtomicBoolean();
    assertDoesNotThrow(() -> queue.queuePacket(state -> {
      built.set(true);
      return null;
    }));
    assertFalse(built.get());
  }

  @Test
  void dropsPacketWithoutAnyServer() {
    ConnectedPlayer player = mock(ConnectedPlayer.class);
    ChatQueue queue = new ChatQueue(player);
    assertDoesNotThrow(() -> queue.queuePacket(state -> null));
  }
}
