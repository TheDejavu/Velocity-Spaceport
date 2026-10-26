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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.Ticker;
import com.velocitypowered.proxy.network.login.MojangNameLookup.Ownership;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Mojang name lookups for offline logins: one shared HTTP client and a short cache. Only a clear
 * answer is cached. Silence (429, 5xx, timeout) is never cached and never means "free", so the
 * caller still refuses the login.
 */
public final class MojangNames implements AutoCloseable {

  private static final Logger logger = LogManager.getLogger(MojangNames.class);

  static final Duration OWNS_TTL = Duration.ofHours(1);
  static final Duration FREE_TTL = Duration.ofMinutes(5);
  private static final long LOG_EVERY_MILLIS = 30_000;
  private static final Pattern NAME = Pattern.compile("[A-Za-z0-9_]{1,16}");

  /** Lookup counters, for the metrics endpoint. */
  public record Stats(long cacheHits, long owns, long free, long silent) {
  }

  private final HttpClient client;
  private final Function<String, CompletableFuture<Ownership>> backend;
  private final LongSupplier clock;
  private final Cache<String, Ownership> cache;
  private final AtomicLong hits = new AtomicLong();
  private final AtomicLong owns = new AtomicLong();
  private final AtomicLong free = new AtomicLong();
  private final AtomicLong silent = new AtomicLong();
  private final AtomicLong suppressed = new AtomicLong();
  private volatile long lastLog = Long.MIN_VALUE;

  /**
   * Creates a lookup that asks Mojang through {@code client}.
   *
   * @param client the shared client; closed by {@link #close()}
   */
  public MojangNames(HttpClient client) {
    this(client, name -> MojangNameLookup.lookup(client, name), System::currentTimeMillis,
        Ticker.systemTicker());
  }

  MojangNames(HttpClient client, Function<String, CompletableFuture<Ownership>> backend,
      LongSupplier clock, Ticker ticker) {
    this.client = client;
    this.backend = backend;
    this.clock = clock;
    this.cache = Caffeine.newBuilder().maximumSize(10_000).ticker(ticker)
        .expireAfter(new Expiry<String, Ownership>() {
          @Override
          public long expireAfterCreate(String key, Ownership value, long now) {
            return (value == Ownership.OWNS ? OWNS_TTL : FREE_TTL).toNanos();
          }

          @Override
          public long expireAfterUpdate(String key, Ownership value, long now, long left) {
            return expireAfterCreate(key, value, now);
          }

          @Override
          public long expireAfterRead(String key, Ownership value, long now, long left) {
            return left;
          }
        }).build();
  }

  /**
   * Whether anyone owns the name.
   *
   * @param name the login name
   * @return the answer; {@link Ownership#SILENT} when Mojang did not give a clear one
   */
  public CompletableFuture<Ownership> lookup(String name) {
    if (name == null || !NAME.matcher(name).matches()) {
      silent.incrementAndGet();
      return CompletableFuture.completedFuture(Ownership.SILENT);
    }
    String key = name.toLowerCase(Locale.ROOT);
    Ownership known = cache.getIfPresent(key);
    if (known != null) {
      hits.incrementAndGet();
      return CompletableFuture.completedFuture(known);
    }
    CompletableFuture<Ownership> asked;
    try {
      asked = backend.apply(name);
    } catch (RuntimeException e) {
      asked = CompletableFuture.failedFuture(e);
    }
    if (asked == null) {
      asked = CompletableFuture.completedFuture(Ownership.SILENT);
    }
    return asked.handle((answer, error) -> {
      Ownership result = error == null && answer != null ? answer : Ownership.SILENT;
      switch (result) {
        case OWNS -> owns.incrementAndGet();
        case FREE -> free.incrementAndGet();
        default -> silent.incrementAndGet();
      }
      if (result == Ownership.SILENT) {
        logSilence(name, error);
      } else {
        cache.put(key, result);
      }
      return result;
    });
  }

  private void logSilence(String name, Throwable error) {
    long now = clock.getAsLong();
    if (lastLog != Long.MIN_VALUE && now - lastLog < LOG_EVERY_MILLIS) {
      suppressed.incrementAndGet();
      return;
    }
    lastLog = now;
    long skipped = suppressed.getAndSet(0);
    logger.warn("Mojang name lookup gave no clear answer for {} (429 so far: {}, {} more since the"
            + " last message), refusing the login{}", name, MojangNameLookup.rateLimited(),
        skipped, error == null ? "" : ": " + error);
  }

  public Stats stats() {
    return new Stats(hits.get(), owns.get(), free.get(), silent.get());
  }

  @Override
  public void close() {
    client.close();
  }
}
