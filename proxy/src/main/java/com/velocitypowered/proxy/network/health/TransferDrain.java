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

package com.velocitypowered.proxy.network.health;

import com.velocitypowered.api.network.ProtocolVersion;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.messages.MinecraftChannelIdentifier;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.network.cluster.ClusterService;
import com.velocitypowered.proxy.protocol.StateRegistry;
import com.velocitypowered.proxy.protocol.packet.ClientboundStoreCookiePacket;
import com.velocitypowered.proxy.protocol.packet.TransferPacket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import net.kyori.adventure.text.Component;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: moves every player off a draining proxy, {@code drain-rate} per second. Each one
 * gets a signed ticket as the {@code spaceport:route} cookie and a Transfer to the load balancer,
 * which sends it to a proxy that is not draining; that proxy sends it back to the same backend
 * server. Clients older than 1.20.5 cannot be transferred: they are disconnected with a reconnect
 * message once everyone else has gone.
 */
final class TransferDrain {

  private static final Logger logger = LogManager.getLogger(TransferDrain.class);
  static final MinecraftChannelIdentifier SAVE = MinecraftChannelIdentifier.create("spaceport",
      "save");
  static final String SAVED = "spaceport:saved";
  static final Component RECONNECT =
      Component.translatable("velocity.kick.spaceport-reconnect");

  private final VelocityServer server;
  private final ClusterService cluster;
  private final ScheduledExecutorService timer;
  private final CompletableFuture<Void> done = new CompletableFuture<>();
  private final Set<UUID> sent = new HashSet<>();
  private final Queue<ConnectedPlayer> queue = new ArrayDeque<>();
  private final Map<UUID, CompletableFuture<Void>> saves = new ConcurrentHashMap<>();
  private final LongAdder transferred;
  private final long deadline;
  private boolean oldClientsKicked;
  private @Nullable ScheduledFuture<?> task;

  TransferDrain(VelocityServer server, ClusterService cluster, ScheduledExecutorService timer,
      LongAdder transferred, long deadline) {
    this.server = server;
    this.cluster = cluster;
    this.timer = timer;
    this.transferred = transferred;
    this.deadline = deadline;
  }

  private NetworkConfig config() {
    return server.getConfiguration().getNetworkConfig();
  }

  /**
   * Starts transferring.
   *
   * @return completes when nobody is left, or at the deadline
   */
  CompletableFuture<Void> start() {
    long period = Math.max(1, 1_000_000 / Math.max(1, config().drainRate()));
    logger.info("Draining: transferring {} players to {}:{}, {} per second",
        server.getPlayerCount(), config().transferHost(), config().transferPort(),
        config().drainRate());
    task = timer.scheduleAtFixedRate(this::step, 0, period, TimeUnit.MICROSECONDS);
    return done;
  }

  void cancel() {
    if (task != null) {
      task.cancel(false);
    }
    done.complete(null);
  }

  private void step() {
    try {
      if (done.isDone()) {
        return;
      }
      if (System.currentTimeMillis() >= deadline) {
        logger.warn("Drain timeout reached with {} players left", server.getPlayerCount());
        cancel();
        return;
      }
      ConnectedPlayer next = nextPlayer();
      if (next != null) {
        transfer(next);
        return;
      }
      // Everyone who can be transferred has been; the rest cannot (D6).
      if (!oldClientsKicked) {
        oldClientsKicked = true;
        int n = 0;
        for (Player p : server.getAllPlayers()) {
          if (!sent.contains(p.getUniqueId()) && !canTransfer((ConnectedPlayer) p)) {
            p.disconnect(RECONNECT);
            n++;
          }
        }
        if (n > 0) {
          logger.info("Draining: disconnected {} players whose clients cannot be transferred", n);
        }
      }
      if (server.getPlayerCount() == 0) {
        logger.info("Draining: every player has left ({} transferred)", sent.size());
        cancel();
      }
    } catch (RuntimeException e) {
      logger.error("Drain step failed", e);
    }
  }

  /** The next player to transfer, refilling from the online players (late arrivals too). */
  private @Nullable ConnectedPlayer nextPlayer() {
    if (queue.isEmpty()) {
      for (Player p : server.getAllPlayers()) {
        ConnectedPlayer player = (ConnectedPlayer) p;
        if (!sent.contains(player.getUniqueId()) && canTransfer(player) && ready(player)) {
          queue.add(player);
        }
      }
    }
    return queue.poll();
  }

  private static boolean canTransfer(ConnectedPlayer player) {
    return player.getProtocolVersion().noLessThan(ProtocolVersion.MINECRAFT_1_20_5);
  }

  /** Transfer works in configuration and play; a player still logging in waits. */
  private static boolean ready(ConnectedPlayer player) {
    StateRegistry state = player.getConnection().getState();
    return player.isActive() && (state == StateRegistry.PLAY || state == StateRegistry.CONFIG);
  }

  private void transfer(ConnectedPlayer player) {
    UUID uuid = player.getUniqueId();
    sent.add(uuid);
    VelocityServerConnection backend = player.getConnectedServer();
    String serverName = backend == null ? null : backend.getServerInfo().getName();
    CompletableFuture<Void> saved = config().drainWaitForSave() && backend != null
        ? requestSave(player, backend) : CompletableFuture.completedFuture(null);
    saved.thenCompose(v -> cluster.releaseForTransfer(uuid)).thenRun(() -> {
      byte[] ticket = cluster.issueTicket(uuid, player.getUsername(), serverName,
          player.getRemoteAddress().getAddress().getHostAddress());
      MinecraftConnection connection = player.getConnection();
      if (ticket == null || connection.isClosed()) {
        return;
      }
      // Written straight to the connection, in this order: the cookie must reach the client
      // before the Transfer makes it leave.
      connection.write(new ClientboundStoreCookiePacket(ClusterService.ROUTE_COOKIE, ticket));
      connection.write(new TransferPacket(config().transferHost(), config().transferPort()));
      transferred.increment();
    });
  }

  /** Asks the backend to save the player; waits at most 2 s for {@code spaceport:saved}. */
  private CompletableFuture<Void> requestSave(ConnectedPlayer player,
      VelocityServerConnection backend) {
    CompletableFuture<Void> ack = new CompletableFuture<>();
    saves.put(player.getUniqueId(), ack);
    backend.sendPluginMessage(SAVE,
        player.getUniqueId().toString().getBytes(StandardCharsets.UTF_8));
    return ack.completeOnTimeout(null, 2, TimeUnit.SECONDS)
        .whenComplete((v, ex) -> saves.remove(player.getUniqueId()));
  }

  /** The backend answered {@code spaceport:saved} for this player. */
  void saved(UUID player) {
    CompletableFuture<Void> ack = saves.get(player);
    if (ack != null) {
      ack.complete(null);
    }
  }
}
