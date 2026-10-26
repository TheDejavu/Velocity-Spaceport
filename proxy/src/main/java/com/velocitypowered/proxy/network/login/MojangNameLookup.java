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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asks Mojang whether a name is a paid account before an offline login is offered. Silence is
 * not treated as "cracked".
 */
public final class MojangNameLookup {

  public enum Ownership {
    OWNS,
    FREE,
    SILENT
  }

  static final String LOOKUP = "https://api.mojang.com/users/profiles/minecraft/";

  private static final AtomicLong RATE_LIMITED = new AtomicLong();

  private MojangNameLookup() {
  }

  /**
   * How many lookups Mojang answered with 429 since start.
   *
   * @return the count
   */
  public static long rateLimited() {
    return RATE_LIMITED.get();
  }

  public static Ownership decide(int status) {
    if (status == 200) {
      return Ownership.OWNS;
    }
    if (status == 204 || status == 404) {
      return Ownership.FREE;
    }
    return Ownership.SILENT;
  }

  /**
   * A Floodgate name: the prefix plus a Minecraft name. It cannot be a Mojang account.
   *
   * @param name the login name
   * @param prefix Floodgate's username prefix
   * @return whether the name belongs to that prefix
   */
  public static boolean bedrockName(String name, String prefix) {
    if (name == null || prefix == null || prefix.isEmpty() || name.length() > 16
        || !name.startsWith(prefix) || name.length() == prefix.length()) {
      return false;
    }
    return name.substring(prefix.length()).matches("[A-Za-z0-9_]{1,16}");
  }

  /**
   * A Bedrock name on a proxy that has a Geyser address. It is a cracked account: no Mojang
   * session and no premium stamp, whatever a login plugin asked for.
   *
   * @param name the login name
   * @param host this proxy's Geyser address; empty means Bedrock names stay closed
   * @param prefix Floodgate's username prefix
   * @return whether the login stays cracked
   */
  public static boolean crackedBedrock(String name, String host, String prefix) {
    return host != null && !host.isEmpty() && bedrockName(name, prefix);
  }

  public static CompletableFuture<Ownership> lookup(HttpClient client, String name) {
    if (name == null || !name.matches("[A-Za-z0-9_]{1,16}")) {
      return CompletableFuture.completedFuture(Ownership.SILENT);
    }
    HttpRequest request = HttpRequest.newBuilder(URI.create(LOOKUP + name))
        .timeout(Duration.ofSeconds(5))
        .header("Accept", "application/json")
        .GET()
        .build();
    return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
        .thenApply(response -> {
          if (response.statusCode() == 429) {
            RATE_LIMITED.incrementAndGet();
          }
          return decide(response.statusCode());
        })
        .exceptionally(error -> Ownership.SILENT);
  }
}
