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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.network.login.LoginGate.Decision;
import com.velocitypowered.proxy.network.login.LoginGate.MojangName;
import com.velocitypowered.proxy.network.login.LoginGate.PublicAction;
import com.velocitypowered.proxy.network.login.LoginGate.Session;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LoginGateTest {

  private static final UUID MOJANG = UUID.fromString("853c80ef-3c37-49fd-aa49-938b674adae6");
  private static final UUID OFFLINE = UUID.fromString("069a79f4-44e9-4726-a5be-fca90e38aaf5");
  private static final UUID CLIENT = UUID.fromString("11111111-1111-1111-1111-111111111111");
  private static final List<String> LOBBIES = List.of("lobby");

  @Test
  void existingAccountRejectsEveryPublicAction() {
    for (PublicAction action : PublicAction.values()) {
      Session paid = existingPaid();
      Session cracked = existingCracked();
      assertEquals(Decision.DROP, LoginGate.attempt(paid, action), action.name());
      assertEquals(Decision.DROP, LoginGate.attempt(cracked, action), action.name());
      assertFalse(paid.open(), action.name());
      assertFalse(cracked.open(), action.name());
      assertFalse(paid.premiumLinked(), action.name());
    }
  }

  @Test
  void passwordIsTheOnlyWayThroughAnExistingAccount() {
    Session paid = existingPaid();
    assertEquals(Decision.DROP, LoginGate.offerPassword(paid, false));
    assertFalse(paid.open());
    assertFalse(paid.premiumLinked());

    assertEquals(Decision.ALLOW, LoginGate.offerPassword(paid, true));
    assertTrue(paid.open());
    assertTrue(paid.premiumLinked());
    assertEquals(MOJANG, paid.playerId());

    Session cracked = existingCracked();
    assertEquals(Decision.ALLOW, LoginGate.offerPassword(cracked, true));
    assertTrue(cracked.open());
    assertFalse(cracked.premiumLinked());
    assertEquals(OFFLINE, cracked.playerId());
  }

  @Test
  void signedForceLoginDoesNotSkipThePassword() {
    Session paid = existingPaid();
    assertEquals(Decision.DROP, LoginGate.attempt(paid, PublicAction.SIGNED_FORCE_LOGIN));
    assertFalse(paid.open());
    assertFalse(paid.premiumLinked());
  }

  @Test
  void mojangSilenceOrAFailedPaidLoginKicks() {
    assertNull(LoginGate.admit(MojangName.SILENT, false, MOJANG, OFFLINE, CLIENT, false, false));
    assertNull(LoginGate.admit(MojangName.SILENT, true, MOJANG, OFFLINE, CLIENT, true, true));
    assertNull(LoginGate.admit(MojangName.OWNS, false, MOJANG, OFFLINE, CLIENT, true, false));
    assertNull(LoginGate.admit(MojangName.OWNS, false, MOJANG, OFFLINE, CLIENT, false, false));
  }

  @Test
  void clientIdIsNeverTheAccount() {
    Session paid = existingPaid();
    Session cracked = existingCracked();
    Session freshPaid = LoginGate.admit(MojangName.OWNS, true, MOJANG, OFFLINE, CLIENT, false, false);
    Session freshCracked = LoginGate.admit(MojangName.FREE, false, MOJANG, OFFLINE, CLIENT, false, false);
    assertNotEquals(CLIENT, paid.playerId());
    assertNotEquals(CLIENT, cracked.playerId());
    assertEquals(MOJANG, freshPaid.playerId());
    assertEquals(OFFLINE, freshCracked.playerId());
    assertNotEquals(CLIENT, freshPaid.playerId());
    assertNotEquals(CLIENT, freshCracked.playerId());
  }

  @Test
  void serverSwitchWaitsUntilAuthMeLogsThemIn() {
    Session session = LoginGate.untilAuthMe(OFFLINE);
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.SERVER_SWITCH));
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.PROXY_COMMAND));
    assertEquals(Decision.ALLOW, LoginGate.authMeLoggedIn(session));
    assertTrue(session.open());
    assertEquals(Decision.ALLOW, LoginGate.attempt(session, PublicAction.SERVER_SWITCH));
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.UNSIGNED_FORCE_LOGIN));
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.CLIENT_UUID));
    assertEquals(OFFLINE, session.playerId());
    assertNull(LoginGate.untilAuthMe(null));
    assertEquals(Decision.KICK, LoginGate.authMeLoggedIn(null));
  }

  @Test
  void linkedPaidAccountSkipsThePassword() {
    Session linked = LoginGate.admit(MojangName.OWNS, true, MOJANG, OFFLINE, CLIENT, true, true);
    assertTrue(linked.open());
    assertTrue(linked.premiumLinked());
    assertEquals(Decision.ALLOW, LoginGate.attempt(linked, PublicAction.SERVER_SWITCH));
    assertEquals(Decision.ALLOW, LoginGate.attempt(linked, PublicAction.JOIN_EFFECT));
    assertEquals(Decision.DROP, LoginGate.attempt(linked, PublicAction.UNSIGNED_FORCE_LOGIN));
    assertEquals(Decision.DROP, LoginGate.attempt(linked, PublicAction.CLIENT_UUID));
  }

  @Test
  void newPaidAccountOpensWithoutAPassword() {
    Session fresh = LoginGate.admit(MojangName.OWNS, true, MOJANG, OFFLINE, CLIENT, false, false);
    assertTrue(fresh.open());
    assertTrue(fresh.premiumVerified());
    assertFalse(fresh.registered());
  }

  @Test
  void registerCannotCoverAnExistingOrPaidName() {
    assertEquals(Decision.DROP, LoginGate.offerRegister(existingPaid()));
    assertEquals(Decision.DROP, LoginGate.offerRegister(existingCracked()));
    Session freshCracked = LoginGate.admit(MojangName.FREE, false, MOJANG, OFFLINE, CLIENT, false, false);
    assertEquals(Decision.ALLOW, LoginGate.offerRegister(freshCracked));
    assertTrue(freshCracked.open());
    assertTrue(freshCracked.registered());
    assertEquals(OFFLINE, freshCracked.playerId());
  }

  private static Session existingPaid() {
    Session session = LoginGate.admit(MojangName.OWNS, true, MOJANG, OFFLINE, CLIENT, true, false);
    assertFalse(session.open());
    return session;
  }

  private static Session existingCracked() {
    Session session = LoginGate.admit(MojangName.FREE, false, MOJANG, OFFLINE, CLIENT, true, false);
    assertFalse(session.open());
    return session;
  }

  @Test
  void networkWithoutALoginBackendNeverLocksTheSwitch() {
    Session session = LoginGate.start(false, OFFLINE);
    assertTrue(session.open());
    assertEquals(Decision.ALLOW, LoginGate.attempt(session, PublicAction.SERVER_SWITCH));
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.UNSIGNED_FORCE_LOGIN));
    assertEquals(OFFLINE, session.playerId());
    assertNull(LoginGate.start(false, null));
  }

  @Test
  void gatedNetworkStaysClosedUntilTheBackendReportsTheLogin() {
    Session session = LoginGate.start(true, OFFLINE);
    assertFalse(session.open());
    assertEquals(Decision.DROP, LoginGate.attempt(session, PublicAction.SERVER_SWITCH));
    LoginGate.authMeLoggedIn(session);
    assertEquals(Decision.ALLOW, LoginGate.attempt(session, PublicAction.SERVER_SWITCH));
  }

  @Test
  void loginGateIsOffByDefaultAndReadFromConfig() {
    assertFalse(VelocityConfiguration.NetworkConfig.DEFAULT.loginGate());
    assertFalse(VelocityConfiguration.NetworkConfig.fromConfig(
        com.electronwill.nightconfig.core.CommentedConfig.inMemory()).loginGate());
    com.electronwill.nightconfig.core.CommentedConfig on =
        com.electronwill.nightconfig.core.CommentedConfig.inMemory();
    on.set("login-gate", true);
    assertTrue(VelocityConfiguration.NetworkConfig.fromConfig(on).loginGate());
  }

  @Test
  void proxyFailoverMovesAnUnacceptedPlayerOnlyBetweenTryListServers() {
    List<String> tryList = List.of("auth-1", "auth-2");
    Session locked = LoginGate.untilAuthMe(OFFLINE);
    assertTrue(LoginGate.mayMove(locked, false, false, "lobby", tryList, LOBBIES));
    assertTrue(LoginGate.mayMove(locked, true, true, "auth-2", tryList, LOBBIES));
    assertTrue(LoginGate.mayMove(locked, true, true, "AUTH-2", tryList, LOBBIES));
    assertFalse(LoginGate.mayMove(locked, true, true, "lobby", tryList, LOBBIES));
    assertFalse(LoginGate.mayMove(locked, true, true, null, tryList, LOBBIES));
    assertFalse(LoginGate.mayMove(locked, true, true, "auth-2", List.of(), LOBBIES));
    assertFalse(LoginGate.mayMove(null, true, true, "auth-2", tryList, LOBBIES));
  }

  @Test
  void playerAndPluginMovesStayBlockedEvenToATryListServer() {
    List<String> tryList = List.of("auth-1", "auth-2");
    Session locked = LoginGate.untilAuthMe(OFFLINE);
    assertFalse(LoginGate.mayMove(locked, true, false, "auth-2", tryList, LOBBIES));
    assertFalse(LoginGate.mayMove(locked, true, false, "lobby", tryList, LOBBIES));
  }

  @Test
  void acceptedPlayerMovesAnywhere() {
    Session session = LoginGate.untilAuthMe(OFFLINE);
    LoginGate.authMeLoggedIn(session);
    assertTrue(LoginGate.mayMove(session, true, false, "lobby", List.of("auth-1"), LOBBIES));
    assertTrue(LoginGate.mayMove(session, true, true, "lobby", List.of("auth-1"), LOBBIES));
  }

  @Test
  void aMainLobbyIsNeverAFailoverTargetBeforeLogin() {
    Session locked = LoginGate.untilAuthMe(OFFLINE);
    assertFalse(LoginGate.mayMove(locked, true, true, "lobby", List.of("auth-1", "lobby"), LOBBIES));
    assertTrue(LoginGate.mayMove(locked, true, true, "auth-1", List.of("auth-1", "lobby"), LOBBIES));
  }

  @Test
  void onlyTheProxysOwnUnchangedRedirectCountsAsFailover() {
    Object auth2 = new Object();
    assertTrue(LoginGate.isProxyFailover(auth2, auth2, false));
    assertFalse(LoginGate.isProxyFailover(auth2, auth2, true));
    assertFalse(LoginGate.isProxyFailover(new Object(), auth2, false));
    assertFalse(LoginGate.isProxyFailover(auth2, null, false));
    assertFalse(LoginGate.isProxyFailover(null, null, false));
  }

  @Test
  void aDestinationChangedByAPluginIsGatedByWhereItReallyGoes() {
    // The check runs again on the final destination with the same failover flag.
    List<String> tryList = List.of("auth-1", "auth-2");
    Session locked = LoginGate.untilAuthMe(OFFLINE);
    assertTrue(LoginGate.mayMove(locked, true, true, "auth-2", tryList, LOBBIES));
    assertFalse(LoginGate.mayMove(locked, true, true, "survival", tryList, LOBBIES));
  }

  @Test
  void startupWarnsWhenTheNetworkIsOnWithoutTheLoginGate() {
    VelocityConfiguration.NetworkConfig base = VelocityConfiguration.NetworkConfig.DEFAULT;
    assertFalse(base.loginGateMissing());
    com.electronwill.nightconfig.core.CommentedConfig c =
        com.electronwill.nightconfig.core.CommentedConfig.inMemory();
    c.set("enabled", true);
    assertTrue(VelocityConfiguration.NetworkConfig.fromConfig(c).loginGateMissing());
    c.set("login-gate", true);
    assertFalse(VelocityConfiguration.NetworkConfig.fromConfig(c).loginGateMissing());
  }
}
