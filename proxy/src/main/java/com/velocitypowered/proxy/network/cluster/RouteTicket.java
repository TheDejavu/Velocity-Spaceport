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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: the signed ticket a draining proxy hands a player (as the {@code spaceport:route}
 * cookie) before transferring it. The proxy that receives the player checks it before
 * encryption: a valid ticket lets the player skip the second Mojang check and routes it back to
 * the same backend server.
 *
 * <p>Wire format: version byte, key id, fields, then HMAC-SHA256 over everything before it. The
 * skin is not inside (cookies are limited to 5 KiB); it comes from the shared profile cache.
 *
 * @param uuid the player
 * @param name the player's name
 * @param server the backend server the player was on, or empty
 * @param ip the player's address; the ticket is only valid from it
 * @param issued when it was signed, epoch millis
 * @param expires when it stops being valid, epoch millis
 * @param nonce single-use id
 * @param from the proxy that signed it
 */
public record RouteTicket(UUID uuid, String name, String server, String ip, long issued,
                          long expires, String nonce, String from) {

  static final byte VERSION = 1;
  /** A ticket is valid this long after it was signed. */
  public static final long LIFETIME_MILLIS = 30_000;
  /** Tolerated clock difference between proxies. */
  static final long SKEW_MILLIS = 5_000;
  private static final int MAC_LENGTH = 32;
  private static final SecureRandom RANDOM = new SecureRandom();

  /**
   * Creates a ticket valid for {@link #LIFETIME_MILLIS} from now with a fresh nonce.
   *
   * @param uuid the player
   * @param name its name
   * @param server its backend server, or empty
   * @param ip its address
   * @param from this proxy
   * @param now the time, epoch millis
   * @return the ticket
   */
  public static RouteTicket issue(UUID uuid, String name, String server, String ip, String from,
      long now) {
    byte[] nonce = new byte[16];
    RANDOM.nextBytes(nonce);
    return new RouteTicket(uuid, name, server, ip, now, now + LIFETIME_MILLIS,
        Base64.getUrlEncoder().withoutPadding().encodeToString(nonce), from);
  }

  /**
   * Signs this ticket with the current key.
   *
   * @param keys the keys
   * @return the cookie payload
   */
  public byte[] sign(Keys keys) {
    byte[] body = MessageBus.encode(out -> {
      out.writeByte(VERSION);
      out.writeUTF(keys.currentId());
      out.writeLong(uuid.getMostSignificantBits());
      out.writeLong(uuid.getLeastSignificantBits());
      out.writeUTF(name);
      out.writeUTF(server);
      out.writeUTF(ip);
      out.writeLong(issued);
      out.writeLong(expires);
      out.writeUTF(nonce);
      out.writeUTF(from);
    });
    byte[] mac = keys.mac(keys.currentId(), body);
    byte[] signed = Arrays.copyOf(body, body.length + MAC_LENGTH);
    System.arraycopy(mac, 0, signed, body.length, MAC_LENGTH);
    return signed;
  }

  /**
   * Reads and checks a signed ticket: known key, valid signature, not expired, not from the
   * future. The single-use nonce is checked by the caller against the store.
   *
   * @param signed the cookie payload
   * @param keys the keys
   * @param now the time, epoch millis
   * @return the ticket, or empty if anything is wrong with it
   */
  public static Optional<RouteTicket> verify(byte @Nullable [] signed, Keys keys, long now) {
    if (signed == null || signed.length <= MAC_LENGTH + 1) {
      return Optional.empty();
    }
    byte[] body = Arrays.copyOf(signed, signed.length - MAC_LENGTH);
    byte[] mac = Arrays.copyOfRange(signed, signed.length - MAC_LENGTH, signed.length);
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
      if (in.readByte() != VERSION) {
        return Optional.empty();
      }
      String kid = in.readUTF();
      byte[] expected = keys.mac(kid, body);
      if (expected == null || !MessageDigest.isEqual(expected, mac)) {
        return Optional.empty();
      }
      RouteTicket ticket = new RouteTicket(new UUID(in.readLong(), in.readLong()), in.readUTF(),
          in.readUTF(), in.readUTF(), in.readLong(), in.readLong(), in.readUTF(), in.readUTF());
      if (now > ticket.expires() || ticket.issued() > now + SKEW_MILLIS) {
        return Optional.empty();
      }
      return Optional.of(ticket);
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  /**
   * HMAC keys by id; the first one signs, all of them verify (rotation: add the new key on every
   * proxy as a second line, then move it to the first line, then drop the old one).
   */
  public static final class Keys {
    private final Map<String, byte[]> keys;
    private final String current;

    /**
     * Creates the key set.
     *
     * @param keys keys by id, the first one signs
     */
    public Keys(Map<String, byte[]> keys) {
      if (keys.isEmpty()) {
        throw new IllegalArgumentException("no transfer ticket keys");
      }
      this.keys = new LinkedHashMap<>(keys);
      this.current = keys.keySet().iterator().next();
    }

    String currentId() {
      return current;
    }

    byte @Nullable [] mac(String kid, byte[] data) {
      byte[] key = keys.get(kid);
      if (key == null) {
        return null;
      }
      try {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data);
      } catch (GeneralSecurityException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Loads keys from a file of {@code id=base64} lines, creating it with one random key if it
     * does not exist.
     *
     * @param file the file
     * @return the keys
     * @throws IOException if the file cannot be read or written
     */
    public static Keys load(Path file) throws IOException {
      if (!Files.exists(file)) {
        byte[] key = new byte[32];
        RANDOM.nextBytes(key);
        Files.writeString(file, "k1=" + Base64.getEncoder().encodeToString(key) + "\n",
            StandardCharsets.UTF_8);
      }
      Map<String, byte[]> keys = new LinkedHashMap<>();
      List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
      for (String line : lines) {
        String trimmed = line.strip();
        int eq = trimmed.indexOf('=');
        if (trimmed.isEmpty() || trimmed.startsWith("#") || eq <= 0) {
          continue;
        }
        byte[] key = Base64.getDecoder().decode(trimmed.substring(eq + 1).strip());
        if (key.length < 16) {
          throw new IOException("transfer key " + trimmed.substring(0, eq) + " is too short");
        }
        keys.put(trimmed.substring(0, eq).strip(), key);
      }
      return new Keys(keys);
    }
  }
}
