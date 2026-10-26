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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.velocitypowered.proxy.network.login.MojangNameLookup.Ownership;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MojangNamesTest {

  private final HttpClient client = mock(HttpClient.class);
  private final AtomicLong nanos = new AtomicLong();
  private final Deque<CompletableFuture<Ownership>> answers = new ArrayDeque<>();
  private int asked;
  private final MojangNames names = new MojangNames(client, name -> {
    asked++;
    return answers.poll();
  }, () -> nanos.get() / 1_000_000, nanos::get);

  private Ownership ask(String name, CompletableFuture<Ownership> answer) {
    answers.add(answer);
    return names.lookup(name).join();
  }

  @Test
  void aClearAnswerIsCachedCaseInsensitively() {
    assertEquals(Ownership.OWNS, ask("Notch", CompletableFuture.completedFuture(Ownership.OWNS)));
    assertEquals(Ownership.OWNS, names.lookup("NOTCH").join());
    assertEquals(Ownership.OWNS, names.lookup("notch").join());
    assertEquals(1, asked);
    assertEquals(2, names.stats().cacheHits());
    assertEquals(1, names.stats().owns());
  }

  @Test
  void aFreeNameIsRememberedOnlyBrieflyAndAnOwnedNameLonger() {
    ask("a", CompletableFuture.completedFuture(Ownership.FREE));
    ask("b", CompletableFuture.completedFuture(Ownership.OWNS));
    nanos.addAndGet(Duration.ofMinutes(4).toNanos());
    names.lookup("a");
    names.lookup("b");
    assertEquals(2, asked);
    nanos.addAndGet(Duration.ofMinutes(2).toNanos());
    ask("a", CompletableFuture.completedFuture(Ownership.FREE));
    assertEquals(3, asked);
    names.lookup("b");
    assertEquals(3, asked);
    nanos.addAndGet(Duration.ofHours(1).toNanos());
    ask("b", CompletableFuture.completedFuture(Ownership.OWNS));
    assertEquals(4, asked);
  }

  @Test
  void silenceIsNeverCachedAndStaysSilent() {
    assertEquals(Ownership.SILENT, ask("x", CompletableFuture.completedFuture(Ownership.SILENT)));
    assertEquals(Ownership.SILENT, ask("x", CompletableFuture.failedFuture(new RuntimeException())));
    assertEquals(Ownership.SILENT, ask("x", CompletableFuture.completedFuture(null)));
    assertEquals(3, asked);
    assertEquals(3, names.stats().silent());
    assertEquals(0, names.stats().cacheHits());
    assertEquals(Ownership.OWNS, ask("x", CompletableFuture.completedFuture(Ownership.OWNS)));
  }

  @Test
  void aNullNameIsSilent() {
    assertEquals(Ownership.SILENT, names.lookup(null).join());
    assertEquals(0, asked);
  }

  @Test
  void closingClosesTheSharedClient() {
    names.close();
    verify(client).close();
  }

  @Test
  void aThrowingOrNullBackendIsSilentNotAnError() {
    MojangNames throwing = new MojangNames(client, name -> {
      throw new IllegalStateException("boom");
    }, () -> 0, () -> 0);
    assertEquals(Ownership.SILENT, throwing.lookup("Steve").join());
    MojangNames nulls = new MojangNames(client, name -> null, () -> 0, () -> 0);
    assertEquals(Ownership.SILENT, nulls.lookup("Steve").join());
    assertEquals(1, throwing.stats().silent());
  }

  @Test
  void anInvalidNameNeverReachesTheBackendOrTheCache() {
    assertEquals(Ownership.SILENT, names.lookup("bad name\n").join());
    assertEquals(Ownership.SILENT, names.lookup("a".repeat(17)).join());
    assertEquals(Ownership.SILENT, names.lookup("").join());
    assertEquals(0, asked);
    assertEquals(3, names.stats().silent());
  }
}
