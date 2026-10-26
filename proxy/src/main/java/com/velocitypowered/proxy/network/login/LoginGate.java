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
import java.util.UUID;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Decides what a connection may do before the account is proven.
 *
 * <p>A paid session proves the Mojang account and nothing else. An account that already has a
 * password stays shut until that password is offered. The id the client sent is never stored.
 */
public final class LoginGate {

  /** The backend sends this after AuthMe logs the player in. A client packet is never this. */
  public static final String LOGGED_IN_CHANNEL = "spaceport:account";

  /** What reached us from the client, the proxy command map, or a plugin message. */
  public enum PublicAction {
    LOGIN_COMMAND,
    REGISTER_COMMAND,
    PREMIUM_COMMAND,
    OTHER_COMMAND,
    PROXY_COMMAND,
    CHAT,
    PLUGIN_MESSAGE,
    UNSIGNED_FORCE_LOGIN,
    SIGNED_FORCE_LOGIN,
    SERVER_SWITCH,
    JOIN_EFFECT,
    MOVE,
    INTERACT,
    INVENTORY,
    BOOK_OR_SIGN,
    TAB_COMPLETE,
    CLIENT_UUID
  }

  public enum Decision {
    ALLOW,
    DROP,
    KICK
  }

  /** Mojang's answer to "does anyone own this name?". Silence is not a free name. */
  public enum MojangName {
    OWNS,
    FREE,
    SILENT
  }

  private LoginGate() {
  }

  /**
   * Opens a session, or kicks. {@code clientId} is accepted so a caller cannot forget it, and
   * then discarded.
   */
  public static @Nullable Session admit(MojangName mojang, boolean hasJoined, UUID mojangId,
      UUID offlineId, UUID clientId, boolean registered, boolean premiumLinked) {
    if (clientId == null || mojangId == null || offlineId == null) {
      return null;
    }
    if (mojang == MojangName.SILENT) {
      return null;
    }
    if (mojang == MojangName.OWNS) {
      if (!hasJoined) {
        return null;
      }
      boolean open = !registered || premiumLinked;
      return new Session(open, true, registered, premiumLinked, mojangId);
    }
    return new Session(false, false, registered, false, offlineId);
  }

  /** Shut until {@link #authMeLoggedIn} says AuthMe accepted this player. */
  public static @Nullable Session untilAuthMe(UUID playerId) {
    if (playerId == null) {
      return null;
    }
    return new Session(false, false, true, false, playerId);
  }

  /** No login backend: the session starts open, so nothing waits for AuthMe. */
  public static Session ungated(UUID playerId) {
    if (playerId == null) {
      return null;
    }
    return new Session(true, false, true, false, playerId);
  }

  /**
   * The session a new connection starts with.
   *
   * @param gated whether the network runs a login backend that opens the session
   * @param playerId the player's id
   * @return a session locked until AuthMe logs in, or an open one
   */
  public static Session start(boolean gated, UUID playerId) {
    return gated ? untilAuthMe(playerId) : ungated(playerId);
  }

  /** AuthMe on the backend accepted this player. */
  public static Decision authMeLoggedIn(Session session) {
    if (session == null) {
      return Decision.KICK;
    }
    session.open = true;
    return Decision.ALLOW;
  }

  /**
   * Whether a move to another server may start. A player who is not accepted yet moves only
   * when the proxy itself redirects them after a kick, and only to a try-list server (the
   * login limbos), never to a server a player or plugin picked. A limbo is a try-list server
   * that is not also a login lobby, so a failover cannot land on a main lobby unaccepted.
   *
   * @param session the player's session
   * @param joined whether the player has already joined a server
   * @param proxyFailover whether the proxy started this move from a kick or restart
   * @param target the server the move is to
   * @param tryList the servers the proxy falls back through
   * @param lobbies the main login lobbies, never a failover target before login
   * @return whether the move may go ahead
   */
  public static boolean mayMove(Session session, boolean joined, boolean proxyFailover,
      String target, List<String> tryList, List<String> lobbies) {
    if (!joined || attempt(session, PublicAction.SERVER_SWITCH) == Decision.ALLOW) {
      return true;
    }
    return proxyFailover && session != null && LoginLobbies.staying(target, tryList)
        && !LoginLobbies.staying(target, lobbies);
  }

  /**
   * Whether a redirect after a kick is the proxy's own choice. A plugin that builds its own
   * result in the kick event, even for the same server, is not.
   *
   * @param chosen the server the final result redirects to
   * @param proxyChoice the server the proxy picked before the event, or null
   * @param pluginChangedResult whether the final result is not the proxy's default one
   * @return whether the redirect counts as a proxy failover
   */
  public static boolean isProxyFailover(Object chosen, Object proxyChoice,
      boolean pluginChangedResult) {
    return chosen != null && chosen == proxyChoice && !pluginChangedResult;
  }

  /** A client packet, command, or listener. Never opens a locked account. */
  public static Decision attempt(Session session, PublicAction action) {
    if (session == null || action == null) {
      return Decision.KICK;
    }
    if (action == PublicAction.UNSIGNED_FORCE_LOGIN || action == PublicAction.CLIENT_UUID) {
      return Decision.DROP;
    }
    if (!session.open) {
      return Decision.DROP;
    }
    if (action == PublicAction.SIGNED_FORCE_LOGIN) {
      return session.premiumLinked ? Decision.ALLOW : Decision.DROP;
    }
    return Decision.ALLOW;
  }

  /**
   * The only way to open an account that already has a password. A paid session that passes
   * this is linked, so the next join can skip the prompt.
   */
  public static Decision offerPassword(Session session, boolean passwordOk) {
    if (session == null || session.open || !session.registered || !passwordOk) {
      return Decision.DROP;
    }
    session.open = true;
    if (session.premiumVerified) {
      session.premiumLinked = true;
    }
    return Decision.ALLOW;
  }

  /** A cracked name that nobody has registered yet. A paid name cannot be registered over. */
  public static Decision offerRegister(Session session) {
    if (session == null || session.open || session.registered || session.premiumVerified) {
      return Decision.DROP;
    }
    session.open = true;
    session.registered = true;
    return Decision.ALLOW;
  }

  public static final class Session {
    private boolean open;
    private final boolean premiumVerified;
    private boolean registered;
    private boolean premiumLinked;
    private final UUID playerId;

    private Session(boolean open, boolean premiumVerified, boolean registered,
        boolean premiumLinked, UUID playerId) {
      this.open = open;
      this.premiumVerified = premiumVerified;
      this.registered = registered;
      this.premiumLinked = premiumLinked;
      this.playerId = playerId;
    }

    public boolean open() {
      return open;
    }

    public boolean premiumVerified() {
      return premiumVerified;
    }

    public boolean registered() {
      return registered;
    }

    public boolean premiumLinked() {
      return premiumLinked;
    }

    public UUID playerId() {
      return playerId;
    }
  }
}
