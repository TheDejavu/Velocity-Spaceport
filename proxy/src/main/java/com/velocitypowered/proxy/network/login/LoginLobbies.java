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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

/**
 * Main lobbies a player may be moved to once the account is accepted.
 */
public final class LoginLobbies {

  private LoginLobbies() {
  }

  /**
   * A player already standing on one of these lobbies stays there.
   *
   * @param here the server they are on, or null
   * @param lobbies configured main lobbies
   * @return whether they are already on one
   */
  public static boolean staying(String here, List<String> lobbies) {
    if (here == null || lobbies == null) {
      return false;
    }
    for (String lobby : lobbies) {
      if (here.equalsIgnoreCase(lobby)) {
        return true;
      }
    }
    return false;
  }

  /** Tells the player when no main lobby took them. */
  public static final String UNAVAILABLE =
      "No lobby is available right now. Use /server to pick one.";

  /**
   * Walks the main lobbies in order and starts over a few times when none accepts. A search ends
   * with exactly one of {@code arrived} or {@code gaveUp}, unless the player left.
   */
  public static final class Search {

    public static final int ROUNDS = 3;
    public static final long RETRY_SECONDS = 5;

    private final List<String> lobbies;
    private final Function<String, CompletableFuture<Boolean>> mover;
    private final BiConsumer<Runnable, Long> later;
    private final Executor executor;
    private final BooleanSupplier active;
    private final Runnable arrived;
    private final Runnable gaveUp;

    /**
     * Creates a search.
     *
     * @param lobbies the main lobbies, in order
     * @param mover moves the player to a lobby by name; completes true when they arrived
     * @param later runs a task after the given number of seconds
     * @param executor where each step runs
     * @param active whether the player is still connected
     * @param arrived called once a lobby accepted them
     * @param gaveUp called once every round has failed
     */
    public Search(List<String> lobbies, Function<String, CompletableFuture<Boolean>> mover,
        BiConsumer<Runnable, Long> later, Executor executor, BooleanSupplier active,
        Runnable arrived, Runnable gaveUp) {
      this.lobbies = List.copyOf(lobbies);
      this.mover = mover;
      this.later = later;
      this.executor = executor;
      this.active = active;
      this.arrived = arrived;
      this.gaveUp = gaveUp;
    }

    /** Starts the first round. */
    public void start() {
      step(0, 0);
    }

    private void step(int round, int index) {
      if (!active.getAsBoolean()) {
        return;
      }
      if (index >= lobbies.size()) {
        if (round + 1 < ROUNDS) {
          later.accept(() -> step(round + 1, 0), RETRY_SECONDS);
        } else {
          gaveUp.run();
        }
        return;
      }
      mover.apply(lobbies.get(index)).whenComplete((moved, failure) -> {
        if (failure == null && Boolean.TRUE.equals(moved)) {
          arrived.run();
        } else {
          executor.execute(() -> step(round, index + 1));
        }
      });
    }
  }
}
