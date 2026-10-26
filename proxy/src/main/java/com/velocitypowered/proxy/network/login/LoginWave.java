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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How many login handshakes this proxy will accept in one second, from every address together.
 * Zero turns it off. The check is a couple of atomics, before Redis and before any call to Mojang.
 */
public final class LoginWave {

  private final int maxPerSecond;
  private final AtomicLong second = new AtomicLong(-1);
  private final AtomicInteger accepted = new AtomicInteger();

  public LoginWave(int maxPerSecond) {
    this.maxPerSecond = Math.max(0, maxPerSecond);
  }

  /** Whether one more login handshake fits in this second. */
  public boolean allow(long nowMillis) {
    if (maxPerSecond == 0) {
      return true;
    }
    long current = nowMillis / 1000L;
    while (true) {
      long seen = second.get();
      if (seen != current) {
        if (second.compareAndSet(seen, current)) {
          accepted.set(1);
          return true;
        }
        continue;
      }
      int count = accepted.get();
      if (count >= maxPerSecond) {
        return false;
      }
      if (accepted.compareAndSet(count, count + 1)) {
        return true;
      }
    }
  }
}
