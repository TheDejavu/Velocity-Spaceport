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

package com.velocitypowered.proxy.command.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mojang.brigadier.StringReader;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.proxy.protocol.packet.brigadier.ArgumentPropertyRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

class SendTargetArgumentTypeTest {

  @Test
  void readsWordsWithColons() throws Exception {
    StringReader reader = new StringReader("server:lobby hub");
    assertEquals("server:lobby", SendTargetArgumentType.INSTANCE.parse(reader));
    assertEquals(" hub", reader.getRemaining());
    assertEquals("Steve", SendTargetArgumentType.INSTANCE.parse(new StringReader("Steve")));
    assertEquals("a b", SendTargetArgumentType.INSTANCE.parse(new StringReader("\"a b\" hub")));
  }

  @Test
  void clientsSeePlainWord() {
    ByteBuf buf = Unpooled.buffer();
    ArgumentPropertyRegistry.serialize(buf, SendTargetArgumentType.INSTANCE,
        ProtocolVersion.MAXIMUM_VERSION);
    ByteBuf word = Unpooled.buffer();
    ArgumentPropertyRegistry.serialize(word,
        com.mojang.brigadier.arguments.StringArgumentType.word(), ProtocolVersion.MAXIMUM_VERSION);
    assertEquals(word, buf);
  }
}
