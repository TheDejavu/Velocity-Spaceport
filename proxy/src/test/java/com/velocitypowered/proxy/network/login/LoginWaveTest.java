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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LoginWaveTest {

  @Test
  void turnedOffAcceptsAWave() {
    LoginWave wave = new LoginWave(0);
    for (int i = 0; i < 50_000; i++) {
      assertTrue(wave.allow(1_000));
    }
  }

  @Test
  void aSecondOnlyAdmitsItsBudget() {
    LoginWave wave = new LoginWave(3);
    assertTrue(wave.allow(5_100));
    assertTrue(wave.allow(5_200));
    assertTrue(wave.allow(5_900));
    assertFalse(wave.allow(5_950));
    assertTrue(wave.allow(6_000));
  }
}
