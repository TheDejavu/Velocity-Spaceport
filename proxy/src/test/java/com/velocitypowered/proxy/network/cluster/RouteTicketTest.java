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

package com.velocitypowered.proxy.network.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RouteTicketTest {

  private static final long NOW = 1_800_000_000_000L;
  private static final UUID STEVE = UUID.randomUUID();

  private static RouteTicket.Keys keys(String... ids) {
    Map<String, byte[]> keys = new LinkedHashMap<>();
    for (String id : ids) {
      keys.put(id, (id + "-0123456789abcdef-secret").getBytes(StandardCharsets.UTF_8));
    }
    return new RouteTicket.Keys(keys);
  }

  private static RouteTicket ticket() {
    return RouteTicket.issue(STEVE, "Steve", "lobby", "192.0.2.7", "p1", NOW);
  }

  @Test
  void signedTicketVerifies() {
    RouteTicket ticket = ticket();
    byte[] signed = ticket.sign(keys("k1"));
    assertTrue(signed.length < 5 * 1024);
    assertEquals(Optional.of(ticket), RouteTicket.verify(signed, keys("k1"), NOW + 1_000));
  }

  @Test
  void tamperedTicketIsRejected() {
    byte[] signed = ticket().sign(keys("k1"));
    for (int i = 0; i < signed.length; i++) {
      byte[] copy = signed.clone();
      copy[i] ^= 1;
      assertEquals(Optional.empty(), RouteTicket.verify(copy, keys("k1"), NOW), "byte " + i);
    }
    assertEquals(Optional.empty(), RouteTicket.verify(new byte[3], keys("k1"), NOW));
    assertEquals(Optional.empty(), RouteTicket.verify(null, keys("k1"), NOW));
  }

  @Test
  void expiredOrFutureTicketIsRejected() {
    byte[] signed = ticket().sign(keys("k1"));
    assertEquals(Optional.empty(),
        RouteTicket.verify(signed, keys("k1"), NOW + RouteTicket.LIFETIME_MILLIS + 1));
    assertEquals(Optional.empty(),
        RouteTicket.verify(signed, keys("k1"), NOW - RouteTicket.SKEW_MILLIS - 1));
  }

  @Test
  void keyRotation() {
    byte[] oldKey = ticket().sign(keys("k1"));
    // step 1: every proxy knows k2 as well, still signing with k1
    assertTrue(RouteTicket.verify(oldKey, keys("k1", "k2"), NOW).isPresent());
    // step 2: k2 signs, k1 still verifies tickets already handed out
    byte[] newKey = ticket().sign(keys("k2", "k1"));
    assertTrue(RouteTicket.verify(newKey, keys("k2", "k1"), NOW).isPresent());
    assertTrue(RouteTicket.verify(oldKey, keys("k2", "k1"), NOW).isPresent());
    // step 3: k1 dropped
    assertEquals(Optional.empty(), RouteTicket.verify(oldKey, keys("k2"), NOW));
    // an unknown key never verifies
    assertEquals(Optional.empty(), RouteTicket.verify(newKey, keys("k1"), NOW));
  }

  @Test
  void eachTicketHasItsOwnNonce() {
    assertNotEquals(ticket().nonce(), ticket().nonce());
  }

  @Test
  void keyFileIsOwnerOnly(@TempDir Path dir) throws Exception {
    assumeTrue(dir.getFileSystem().supportedFileAttributeViews().contains("posix"),
        "POSIX permissions only");
    Path file = dir.resolve("network-secret.key");
    RouteTicket.Keys.load(file);
    assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)));
  }

  @Test
  void readableKeyFileIsRefused(@TempDir Path dir) throws Exception {
    assumeTrue(dir.getFileSystem().supportedFileAttributeViews().contains("posix"),
        "POSIX permissions only");
    Path file = dir.resolve("network-secret.key");
    RouteTicket.Keys.load(file);
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
    assertThrows(IOException.class, () -> RouteTicket.Keys.load(file));
  }

  @Test
  void keyFileIsCreatedAndReadBack(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("network-secret.key");
    RouteTicket.Keys created = RouteTicket.Keys.load(file);
    assertTrue(Files.readString(file).startsWith("k1="));
    byte[] signed = ticket().sign(created);
    assertTrue(RouteTicket.verify(signed, RouteTicket.Keys.load(file), NOW).isPresent());
  }
}
