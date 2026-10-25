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

package com.velocitypowered.proxy.network;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * SO_REUSEPORT is only used with a native transport.
 */
class ConnectionManagerReusePortTest {

  @Test
  void offStaysOff() {
    for (TransportType type : TransportType.values()) {
      assertFalse(ConnectionManager.reusePort(false, type));
    }
  }

  @Test
  void nioIgnoresIt() {
    assertFalse(ConnectionManager.reusePort(true, TransportType.NIO));
  }

  @Test
  void nativeTransportsUseIt() {
    assertTrue(ConnectionManager.reusePort(true, TransportType.EPOLL));
    assertTrue(ConnectionManager.reusePort(true, TransportType.IO_URING));
    assertTrue(ConnectionManager.reusePort(true, TransportType.KQUEUE));
  }
}
