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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class LoginLobbiesTest {

  @Test
  void aPlayerOnAMainLobbyStaysThere() {
    List<String> lobbies = List.of("lobby-1", "lobby-2");
    assertTrue(LoginLobbies.staying("lobby-2", lobbies));
    assertTrue(LoginLobbies.staying("LOBBY-1", lobbies));
    assertFalse(LoginLobbies.staying("auth-1", lobbies));
    assertFalse(LoginLobbies.staying(null, lobbies));
    assertFalse(LoginLobbies.staying("lobby-1", List.of()));
  }

  /** Runs the search with a fake clock; {@code up} are the lobbies that accept. */
  private static final class Run {
    final List<String> asked = new ArrayList<>();
    final List<Runnable> scheduled = new ArrayList<>();
    final List<Long> delays = new ArrayList<>();
    int arrived;
    int gaveUp;
    boolean active = true;
    final LoginLobbies.Search search;

    Run(List<String> lobbies, Set<String> up) {
      search = new LoginLobbies.Search(lobbies, name -> {
        asked.add(name);
        return CompletableFuture.completedFuture(up.contains(name));
      }, (task, seconds) -> {
        scheduled.add(task);
        delays.add(seconds);
      }, Runnable::run, () -> active, () -> arrived++, () -> gaveUp++);
    }

    void runScheduled() {
      List<Runnable> now = new ArrayList<>(scheduled);
      scheduled.clear();
      now.forEach(Runnable::run);
    }
  }

  @Test
  void searchStopsAtTheFirstLobbyThatAccepts() {
    Run run = new Run(List.of("lobby-1", "lobby-2"), Set.of("lobby-2"));
    run.search.start();
    assertEquals(List.of("lobby-1", "lobby-2"), run.asked);
    assertEquals(1, run.arrived);
    assertEquals(0, run.gaveUp);
    assertTrue(run.scheduled.isEmpty());
  }

  @Test
  void searchRetriesBoundedTimesThenGivesUpOnce() {
    Run run = new Run(List.of("lobby-1", "lobby-2"), Set.of());
    run.search.start();
    assertEquals(0, run.gaveUp);
    assertEquals(1, run.scheduled.size());
    run.runScheduled();
    assertEquals(0, run.gaveUp);
    run.runScheduled();
    assertEquals(1, run.gaveUp);
    assertEquals(0, run.arrived);
    assertEquals(6, run.asked.size());
    assertEquals(List.of(LoginLobbies.Search.RETRY_SECONDS, LoginLobbies.Search.RETRY_SECONDS),
        run.delays);
    assertTrue(run.scheduled.isEmpty());
  }

  @Test
  void searchSucceedsOnALaterRound() {
    Set<String> up = new HashSet<>();
    Run run = new Run(List.of("lobby-1"), up);
    run.search.start();
    up.add("lobby-1");
    run.runScheduled();
    assertEquals(1, run.arrived);
    assertEquals(0, run.gaveUp);
  }

  @Test
  void aPlayerWhoLeftIsNotSearchedFor() {
    Run run = new Run(List.of("lobby-1"), Set.of());
    run.search.start();
    run.active = false;
    run.runScheduled();
    assertEquals(0, run.gaveUp);
    assertEquals(0, run.arrived);
    assertEquals(1, run.asked.size());
  }
}
