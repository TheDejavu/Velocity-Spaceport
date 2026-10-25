/*
 * Copyright (C) 2018-2023 Velocity Contributors
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

package com.velocitypowered.proxy.connection.client;

import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static com.velocitypowered.proxy.VelocityServer.GENERAL_GSON;
import static com.velocitypowered.proxy.connection.VelocityConstants.EMPTY_BYTE_ARRAY;
import static com.velocitypowered.proxy.crypto.EncryptionUtils.decryptRsa;
import static com.velocitypowered.proxy.crypto.EncryptionUtils.generateServerId;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.velocitypowered.api.event.connection.PreLoginEvent;
import com.velocitypowered.api.event.connection.PreLoginEvent.PreLoginComponentResult;
import com.velocitypowered.api.network.HandshakeIntent;
import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.crypto.IdentifiedKey;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.MinecraftSessionHandler;
import com.velocitypowered.proxy.crypto.IdentifiedKeyImpl;
import com.velocitypowered.proxy.network.cluster.ClusterService;
import com.velocitypowered.proxy.network.cluster.RouteTicket;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.netty.MinecraftDecoder;
import com.velocitypowered.proxy.protocol.packet.ClientboundCookieRequestPacket;
import com.velocitypowered.proxy.protocol.packet.EncryptionRequestPacket;
import com.velocitypowered.proxy.protocol.packet.EncryptionResponsePacket;
import com.velocitypowered.proxy.protocol.packet.LoginPluginResponsePacket;
import com.velocitypowered.proxy.protocol.packet.ServerLoginPacket;
import com.velocitypowered.proxy.protocol.packet.ServerboundCookieResponsePacket;
import com.velocitypowered.proxy.util.VelocityProperties;
import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Handles authenticating the player to Mojang's servers.
 */
public class InitialLoginSessionHandler implements MinecraftSessionHandler {

  private static final boolean OFFLINE_ENCRYPTION =
      VelocityProperties.readBoolean("spaceport.offline-encryption", false);

  private static final Logger logger = LogManager.getLogger(InitialLoginSessionHandler.class);
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final String MOJANG_HASJOINED_URL =
      System.getProperty("mojang.sessionserver",
              "https://sessionserver.mojang.com/session/minecraft/hasJoined")
          .concat("?username=%s&serverId=%s");

  private final VelocityServer server;
  private final MinecraftConnection mcConnection;
  private final LoginInboundConnection inbound;
  private @MonotonicNonNull ServerLoginPacket login;
  private byte[] verify = EMPTY_BYTE_ARRAY;
  private LoginState currentState = LoginState.LOGIN_PACKET_EXPECTED;
  private boolean offlineEncryption;
  // Spaceport: a transferred player's ticket check before encryption
  private @MonotonicNonNull PreLoginComponentResult preLoginResult;
  private @Nullable GameProfile ticketProfile;
  private final boolean forceKeyAuthentication;

  InitialLoginSessionHandler(VelocityServer server, MinecraftConnection mcConnection,
                             LoginInboundConnection inbound) {
    this.server = Preconditions.checkNotNull(server, "server");
    this.mcConnection = Preconditions.checkNotNull(mcConnection, "mcConnection");
    this.inbound = Preconditions.checkNotNull(inbound, "inbound");
    this.forceKeyAuthentication = VelocityProperties.readBoolean(
            "auth.forceSecureProfiles", server.getConfiguration().isForceKeyAuthentication());
  }

  @Override
  public boolean handle(ServerLoginPacket packet) {
    assertState(LoginState.LOGIN_PACKET_EXPECTED);
    this.currentState = LoginState.LOGIN_PACKET_RECEIVED;
    IdentifiedKey playerKey = packet.getPlayerKey();
    if (playerKey != null) {
      if (playerKey.hasExpired()) {
        inbound.disconnect(
            Component.translatable("multiplayer.disconnect.invalid_public_key_signature"));
        return true;
      }

      boolean isKeyValid;
      if (playerKey.getKeyRevision() == IdentifiedKey.Revision.LINKED_V2
          && playerKey instanceof final IdentifiedKeyImpl keyImpl) {
        isKeyValid = keyImpl.internalAddHolder(packet.getHolderUuid());
      } else {
        isKeyValid = playerKey.isSignatureValid();
      }

      if (!isKeyValid) {
        inbound.disconnect(Component.translatable("multiplayer.disconnect.invalid_public_key"));
        return true;
      }
    } else if (mcConnection.getProtocolVersion().noLessThan(ProtocolVersion.MINECRAFT_1_19)
        && forceKeyAuthentication
        && mcConnection.getProtocolVersion().lessThan(ProtocolVersion.MINECRAFT_1_19_3)) {
      inbound.disconnect(Component.translatable("multiplayer.disconnect.missing_public_key"));
      return true;
    }
    inbound.setPlayerKey(playerKey);
    this.login = packet;

    ClusterService cluster = server.getCluster();
    if (cluster == null) {
      firePreLogin();
      return true;
    }
    // Spaceport: the shared per-address rate limit asks the store without blocking; the client
    // waits for our answer, so nothing else can arrive meanwhile. It lets the login through if
    // the store cannot answer.
    String address = ((InetSocketAddress) mcConnection.getRemoteAddress()).getAddress()
        .getHostAddress();
    cluster.allowLogin(address).thenAcceptAsync(allowed -> {
      if (mcConnection.isClosed()) {
        return;
      }
      if (allowed) {
        firePreLogin();
      } else {
        inbound.disconnect(ClusterService.RATE_LIMITED);
      }
    }, mcConnection.eventLoop()).exceptionally(ex -> {
      logger.error("Exception in login rate limit stage", ex);
      return null;
    });
    return true;
  }

  @Override
  public boolean handle(ServerboundCookieResponsePacket packet) {
    if (currentState != LoginState.COOKIE_REQUESTED
        || !packet.getKey().equals(ClusterService.ROUTE_COOKIE)) {
      mcConnection.close(true);
      return true;
    }
    this.currentState = LoginState.LOGIN_PACKET_RECEIVED;
    ClusterService cluster = server.getCluster();
    String ip = ((InetSocketAddress) mcConnection.getRemoteAddress()).getAddress()
        .getHostAddress();
    cluster.redeem(packet.getPayload(), ip, login.getUsername(), login.getHolderUuid())
        .thenAcceptAsync(redeemed -> {
          if (mcConnection.isClosed()) {
            return;
          }
          PreLoginComponentResult result = this.preLoginResult;
          boolean online = server.getConfiguration().isOnlineMode() || result.isOnlineModeAllowed();
          if (redeemed.isPresent() && redeemed.get().nonceChecked() && online
              && server.getConfiguration().getNetworkConfig().skipAuthWithTicket()) {
            RouteTicket ticket = redeemed.get().ticket();
            String properties = redeemed.get().properties();
            this.ticketProfile = new GameProfile(ticket.uuid(), ticket.name(), properties == null
                ? List.of() : List.of(GENERAL_GSON.fromJson(properties,
                    GameProfile.Property[].class)));
            EncryptionRequestPacket request = generateEncryptionRequest();
            request.setShouldAuthenticate(false);
            this.verify = Arrays.copyOf(request.getVerifyToken(), 4);
            mcConnection.write(request);
            this.currentState = LoginState.ENCRYPTION_REQUEST_SENT;
            return;
          }
          continueLogin(result);
        }, mcConnection.eventLoop())
        .exceptionally(ex -> {
          logger.error("Transfer ticket check failed for {}", login.getUsername(), ex);
          mcConnection.close(true);
          return null;
        });
    return true;
  }

  @Override
  public boolean handle(LoginPluginResponsePacket packet) {
    this.inbound.handleLoginPluginResponse(packet);
    return true;
  }

  @Override
  public boolean handle(EncryptionResponsePacket packet) {
    assertState(LoginState.ENCRYPTION_REQUEST_SENT);
    this.currentState = LoginState.ENCRYPTION_RESPONSE_RECEIVED;
    ServerLoginPacket login = this.login;
    if (login == null) {
      throw new IllegalStateException("No ServerLogin packet received yet.");
    }

    if (verify.length == 0) {
      throw new IllegalStateException("No EncryptionRequest packet sent yet.");
    }

    try {
      KeyPair serverKeyPair = server.getServerKeyPair();
      if (inbound.getIdentifiedKey() != null) {
        IdentifiedKey playerKey = inbound.getIdentifiedKey();
        if (!playerKey.verifyDataSignature(packet.getVerifyToken(), verify,
            Longs.toByteArray(packet.getSalt()))) {
          throw new IllegalStateException("Invalid client public signature.");
        }
      } else {
        byte[] decryptedVerifyToken = decryptRsa(serverKeyPair, packet.getVerifyToken());
        if (!MessageDigest.isEqual(verify, decryptedVerifyToken)) {
          throw new IllegalStateException("Unable to successfully decrypt the verification token.");
        }
      }

      byte[] decryptedSharedSecret = decryptRsa(serverKeyPair, packet.getSharedSecret());

      // Go ahead and enable encryption. Once the client sends EncryptionResponse, encryption
      // is enabled.
      mcConnection.enableEncryption(decryptedSharedSecret);
      if (offlineEncryption) {
        mcConnection.setActiveSessionHandler(StateRegistry.LOGIN,
            new AuthSessionHandler(server, inbound,
                GameProfile.forOfflinePlayer(login.getUsername()), false, null));
        return true;
      }
      if (ticketProfile != null) {
        // Spaceport: identity from the transfer ticket, no second Mojang check
        server.getCluster().noteAuthSkipped();
        mcConnection.setActiveSessionHandler(StateRegistry.LOGIN,
            new AuthSessionHandler(server, inbound, ticketProfile, true, null));
        return true;
      }

      String serverId = generateServerId(decryptedSharedSecret, serverKeyPair.getPublic());
      String playerIp = ((InetSocketAddress) mcConnection.getRemoteAddress()).getHostString();
      String url = String.format(MOJANG_HASJOINED_URL,
          urlFormParameterEscaper().escape(login.getUsername()), serverId);

      if (server.getConfiguration().shouldPreventClientProxyConnections()) {
        url += "&ip=" + urlFormParameterEscaper().escape(playerIp);
      }

      final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder();
      if (server.getCluster() != null) {
        // Spaceport: with the network on, a hanging session server counts as down after 10 s
        requestBuilder.timeout(Duration.ofSeconds(10));
      }
      final HttpRequest httpRequest = requestBuilder
              .setHeader("User-Agent",
                      server.getVersion().getName() + "/" + server.getVersion().getVersion())
              .uri(URI.create(url))
              .build();
      //noinspection resource
      final HttpClient httpClient = server.createHttpClient();
      httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
          .whenCompleteAsync((response, throwable) -> {
            if (mcConnection.isClosed()) {
              // The player disconnected after we authenticated them.
              return;
            }

            if (throwable != null) {
              logger.error("Unable to authenticate player", throwable);
              mojangDown(playerIp, serverId);
              return;
            }

            if (response.statusCode() == 200) {
              final GameProfile profile = GENERAL_GSON.fromJson(response.body(),
                  GameProfile.class);
              // Not so fast, now we verify the public key for 1.19.1+
              if (inbound.getIdentifiedKey() != null
                  && inbound.getIdentifiedKey().getKeyRevision() == IdentifiedKey.Revision.LINKED_V2
                  && inbound.getIdentifiedKey() instanceof final IdentifiedKeyImpl key) {
                if (!key.internalAddHolder(profile.getId())) {
                  inbound.disconnect(
                      Component.translatable("multiplayer.disconnect.invalid_public_key"));
                }
              }
              ClusterService cluster = server.getCluster();
              if (cluster != null) {
                cluster.recordGoodLogin(profile.getId(), profile.getName(), playerIp);
              }
              // All went well, initialize the session.
              mcConnection.setActiveSessionHandler(StateRegistry.LOGIN,
                  new AuthSessionHandler(server, inbound, profile, true, serverId));
            } else if (response.statusCode() == 204) {
              // Apparently an offline-mode user logged onto this online-mode proxy.
              inbound.disconnect(
                  Component.translatable("velocity.error.online-mode-only", NamedTextColor.RED));
            } else {
              // Something else went wrong
              logger.error(
                  "Got an unexpected error code {} whilst contacting Mojang to log in {} ({})",
                  response.statusCode(), login.getUsername(), playerIp);
              mojangDown(playerIp, serverId);
            }
          }, mcConnection.eventLoop())
          .whenComplete((ignored, throwable) -> {
            httpClient.close();
          });
    } catch (GeneralSecurityException e) {
      logger.error("Unable to enable encryption", e);
      mcConnection.close(true);
    }
    return true;
  }

  /**
   * Mojang could not confirm the login. Spaceport: in outage mode (D9) a player whose last good
   * login was from this address gets in with its cached profile; otherwise the usual refusal.
   */
  private void mojangDown(String playerIp, String serverId) {
    ClusterService cluster = server.getCluster();
    if (cluster == null || !cluster.authOutageMode()) {
      inbound.disconnect(Component.translatable("multiplayer.disconnect.authservers_down"));
      return;
    }
    cluster.outageLogin(login.getUsername(), playerIp).thenAcceptAsync(profile -> {
      if (mcConnection.isClosed()) {
        return;
      }
      if (profile.isEmpty()) {
        inbound.disconnect(Component.translatable("multiplayer.disconnect.authservers_down"));
        return;
      }
      mcConnection.setActiveSessionHandler(StateRegistry.LOGIN,
          new AuthSessionHandler(server, inbound, profile.get(), true, serverId));
    }, mcConnection.eventLoop());
  }

  private void continueLogin(PreLoginComponentResult result) {
    if (!result.isForceOfflineMode()
        && (server.getConfiguration().isOnlineMode() || result.isOnlineModeAllowed())) {
      // Request encryption.
      EncryptionRequestPacket request = generateEncryptionRequest();
      this.verify = Arrays.copyOf(request.getVerifyToken(), 4);
      mcConnection.write(request);
      this.currentState = LoginState.ENCRYPTION_REQUEST_SENT;
    } else if (OFFLINE_ENCRYPTION
        && mcConnection.getProtocolVersion().noLessThan(ProtocolVersion.MINECRAFT_1_20_5)) {
      // Spaceport: encrypt offline-mode connections too. 1.20.5+ clients skip the Mojang
      // session call when told not to authenticate.
      EncryptionRequestPacket request = generateEncryptionRequest();
      request.setShouldAuthenticate(false);
      this.verify = Arrays.copyOf(request.getVerifyToken(), 4);
      this.offlineEncryption = true;
      mcConnection.write(request);
      this.currentState = LoginState.ENCRYPTION_REQUEST_SENT;
    } else {
      mcConnection.setActiveSessionHandler(StateRegistry.LOGIN,
          new AuthSessionHandler(server, inbound,
              GameProfile.forOfflinePlayer(login.getUsername()), false, null));
    }
  }

  private EncryptionRequestPacket generateEncryptionRequest() {
    byte[] verify = new byte[4];
    SECURE_RANDOM.nextBytes(verify);

    EncryptionRequestPacket request = new EncryptionRequestPacket();
    request.setPublicKey(server.getServerKeyPair().getPublic().getEncoded());
    request.setVerifyToken(verify);
    return request;
  }

  @Override
  public void handleUnknown(ByteBuf buf) {
    mcConnection.close(true);
  }

  @Override
  public void disconnected() {
    this.inbound.cleanup();
  }

  private void assertState(LoginState expectedState) {
    if (this.currentState != expectedState) {
      if (MinecraftDecoder.DEBUG) {
        logger.error("{} Received an unexpected packet requiring state {}, but we are in {}",
            inbound,
            expectedState, this.currentState);
      }
      mcConnection.close(true);
    }
  }

  private enum LoginState {
    LOGIN_PACKET_EXPECTED,
    COOKIE_REQUESTED,
    LOGIN_PACKET_RECEIVED,
    ENCRYPTION_REQUEST_SENT,
    ENCRYPTION_RESPONSE_RECEIVED
  }

  private void firePreLogin() {
    final PreLoginEvent event = new PreLoginEvent(inbound, login.getUsername(), login.getHolderUuid());
    server.getEventManager().fire(event).thenRunAsync(() -> {
      if (mcConnection.isClosed()) {
        // The player was disconnected
        return;
      }

      PreLoginComponentResult result = event.getResult();
      Optional<Component> disconnectReason = result.getReasonComponent();
      if (disconnectReason.isPresent()) {
        // The component is guaranteed to be provided if the connection was denied.
        inbound.disconnect(disconnectReason.get());
        return;
      }

      inbound.loginEventFired(() -> {
        if (mcConnection.isClosed()) {
          // The player was disconnected
          return;
        }

        mcConnection.eventLoop().execute(() -> {
          ClusterService cluster = server.getCluster();
          if (cluster != null && cluster.hasTicketKeys()
              && inbound.getHandshakeIntent() == HandshakeIntent.TRANSFER
              && mcConnection.getProtocolVersion().noLessThan(ProtocolVersion.MINECRAFT_1_20_5)
              && !result.isForceOfflineMode()) {
            // Spaceport: a transferred client may bring a ticket from the proxy it came from.
            // It has to be read before encryption: a client told not to authenticate cannot be
            // checked with Mojang afterwards. The ticket is single use and bound to the address.
            this.preLoginResult = result;
            this.currentState = LoginState.COOKIE_REQUESTED;
            mcConnection.write(new ClientboundCookieRequestPacket(ClusterService.ROUTE_COOKIE));
            return;
          }
          continueLogin(result);
        });
      });
    }, mcConnection.eventLoop()).exceptionally((ex) -> {
      logger.error("Exception in pre-login stage", ex);
      return null;
    });
  }
}
