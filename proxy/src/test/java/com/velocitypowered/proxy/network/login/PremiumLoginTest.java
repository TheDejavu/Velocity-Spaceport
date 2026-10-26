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

package com.velocitypowered.proxy.network.login;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.connection.PlayerDataForwarding;
import com.velocitypowered.proxy.network.login.MojangNameLookup.Ownership;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PremiumLoginTest {

  private static final UUID ID = UUID.fromString("853c80ef-3c37-49fd-aa49-938b674adae6");

  @Test
  void silenceIsNotFreeName() {
    assertEquals(Ownership.OWNS, MojangNameLookup.decide(200));
    assertEquals(Ownership.FREE, MojangNameLookup.decide(204));
    assertEquals(Ownership.FREE, MojangNameLookup.decide(404));
    assertEquals(Ownership.SILENT, MojangNameLookup.decide(429));
    assertEquals(Ownership.SILENT, MojangNameLookup.decide(500));
    assertEquals(Ownership.SILENT, MojangNameLookup.decide(0));
    assertTrue(MojangNameLookup.bedrockName(".Steve", "."));
    assertFalse(MojangNameLookup.bedrockName("Steve", "."));
    assertFalse(MojangNameLookup.bedrockName(".", "."));
    assertFalse(MojangNameLookup.bedrockName(".Steve", ""));
    assertTrue(MojangNameLookup.crackedBedrock(".Steve", "10.0.0.2", "."));
    assertFalse(MojangNameLookup.crackedBedrock(".Steve", "", "."));
    assertFalse(MojangNameLookup.crackedBedrock("Steve", "10.0.0.2", "."));
  }

  @Test
  void stampOnlyMarksProfileThatAlreadyPassedMojang() {
    GameProfile plain = new GameProfile(ID, "Notch", List.of());
    assertFalse(PremiumProof.carries(plain));
    GameProfile stamped = PremiumProof.stamp(plain);
    assertTrue(PremiumProof.carries(stamped));
    assertEquals(plain.getId(), stamped.getId());
    assertTrue(PremiumProof.carries(PremiumProof.stamp(stamped)));
    assertEquals(1, PremiumProof.stamp(stamped).getProperties().size());
  }

  @Test
  void theSignedForwardingBlobCarriesTheProof() {
    byte[] secret = "example-forwarding-secret".getBytes(StandardCharsets.UTF_8);
    GameProfile plain = new GameProfile(ID, "Notch", List.of());
    GameProfile stamped = PremiumProof.stamp(plain);
    ByteBuf plainBlob = forward(secret, plain);
    ByteBuf stampedBlob = forward(secret, stamped);
    try {
      assertNotEquals(read(plainBlob), read(stampedBlob));
      assertTrue(new String(read(stampedBlob), StandardCharsets.UTF_8).contains(PremiumProof.PROPERTY));
      assertFalse(new String(read(plainBlob), StandardCharsets.UTF_8).contains(PremiumProof.PROPERTY));
    } finally {
      plainBlob.release();
      stampedBlob.release();
    }
  }

  @Test
  void clientCannotHandTheProxyFastLoginChannel() {
    assertTrue(PlayerLoginChannels.fromTheClient("fastlogin:force"));
    assertTrue(PlayerLoginChannels.fromTheClient("FastLogin:force"));
    assertTrue(PlayerLoginChannels.fromTheClient("spaceport:account"));
    assertTrue(PlayerLoginChannels.fromTheClient("Spaceport:account"));
    assertFalse(PlayerLoginChannels.fromTheClient("velocity:player_info"));
    assertFalse(PlayerLoginChannels.fromTheClient("minecraft:brand"));
  }

  private static ByteBuf forward(byte[] secret, GameProfile profile) {
    return PlayerDataForwarding.createForwardingData(secret, "127.0.0.1",
        ProtocolVersion.MINECRAFT_1_21_5, profile, null, PlayerDataForwarding.MODERN_DEFAULT);
  }

  private static byte[] read(ByteBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), bytes);
    return bytes;
  }
}
