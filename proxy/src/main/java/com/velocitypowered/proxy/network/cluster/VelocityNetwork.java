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

package com.velocitypowered.proxy.network.cluster;

import com.velocitypowered.api.network.Network;
import com.velocitypowered.api.network.NetworkPlayer;
import com.velocitypowered.api.network.ProxyNode;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link Network} for the API: the cluster when the network is enabled, this proxy alone when not.
 */
public final class VelocityNetwork implements Network {

  private record Node(String getId, int getPlayerCount) implements ProxyNode {
  }

  private final VelocityServer server;
  private final @Nullable ClusterService cluster;
  private final @Nullable NetworkActions remote;
  private final VelocityNetworkPlayer.Actions actions;

  /**
   * Creates the network view.
   *
   * @param server the proxy
   * @param cluster the cluster, or null when the network is off
   * @param remote the cluster's actions, or null when the network is off
   */
  public VelocityNetwork(VelocityServer server, @Nullable ClusterService cluster,
      @Nullable NetworkActions remote) {
    this.server = server;
    this.cluster = cluster;
    this.remote = remote;
    this.actions = remote != null ? remote : new LocalActions();
  }

  public @Nullable ClusterService cluster() {
    return cluster;
  }

  public @Nullable NetworkActions actions() {
    return remote;
  }

  @Override
  public boolean isEnabled() {
    return cluster != null;
  }

  @Override
  public String getProxyId() {
    return cluster == null ? "local" : cluster.proxyId();
  }

  @Override
  public int getPlayerCount() {
    return cluster == null ? server.getPlayerCount() : cluster.getPlayerCount();
  }

  @Override
  public Collection<ProxyNode> getProxies() {
    List<ProxyNode> nodes = new ArrayList<>();
    nodes.add(new Node(getProxyId(), server.getPlayerCount()));
    if (cluster != null) {
      for (ProxyState p : cluster.liveRemotes()) {
        nodes.add(new Node(p.id(), p.players()));
      }
    }
    return nodes;
  }

  @Override
  public CompletableFuture<Optional<NetworkPlayer>> getPlayer(UUID uuid) {
    Optional<Player> local = server.getPlayer(uuid);
    if (local.isPresent() || cluster == null) {
      return CompletableFuture.completedFuture(local.map(this::local));
    }
    return cluster.store().player(uuid).thenApply(r -> r.map(this::wrap));
  }

  @Override
  public CompletableFuture<Optional<NetworkPlayer>> getPlayer(String username) {
    Optional<Player> local = server.getPlayer(username);
    if (local.isPresent() || cluster == null) {
      return CompletableFuture.completedFuture(local.map(this::local));
    }
    return cluster.store().uuidOf(username).thenCompose(id -> id.isEmpty()
        ? CompletableFuture.completedFuture(Optional.<NetworkPlayer>empty())
        : getPlayer(id.get()));
  }

  @Override
  public CompletableFuture<Collection<NetworkPlayer>> getPlayers() {
    if (remote == null) {
      List<NetworkPlayer> players = new ArrayList<>();
      server.getAllPlayers().forEach(p -> players.add(local(p)));
      return CompletableFuture.completedFuture(players);
    }
    return remote.players().thenApply(records -> {
      List<NetworkPlayer> players = new ArrayList<>(records.size());
      records.forEach(r -> players.add(wrap(r)));
      return players;
    });
  }

  @Override
  public CompletableFuture<Void> broadcast(Component message) {
    if (remote == null) {
      server.sendMessage(message);
      return CompletableFuture.completedFuture(null);
    }
    return remote.broadcast(message);
  }

  @Override
  public CompletableFuture<Boolean> executeCommand(String proxyId, String command) {
    if (remote == null) {
      return proxyId.equals(getProxyId())
          ? server.getCommandManager().executeAsync(server.getConsoleCommandSource(), command)
          : CompletableFuture.completedFuture(false);
    }
    return remote.command(proxyId, command);
  }

  /**
   * Wraps a record as an API player.
   *
   * @param record the record
   * @return the player
   */
  public NetworkPlayer wrap(PlayerRecord record) {
    return new VelocityNetworkPlayer(record, actions);
  }

  private NetworkPlayer local(Player p) {
    long since = cluster == null ? 0 : cluster.session(p.getUniqueId())
        .map(s -> s.record.since()).orElse(0L);
    return wrap(new PlayerRecord(p.getUniqueId(), p.getUsername(), getProxyId(), 0,
        p.getCurrentServer().map(ServerConnection::getServerInfo).map(i -> i.getName())
            .orElse(null), p.getRemoteAddress().getAddress().getHostAddress(), since));
  }

  /** Actions on this proxy's own players, when the network is off. */
  private final class LocalActions implements VelocityNetworkPlayer.Actions {
    @Override
    public CompletableFuture<Boolean> message(UUID player, Component message) {
      return CompletableFuture.completedFuture(server.getPlayer(player).map(p -> {
        p.sendMessage(message);
        return true;
      }).orElse(false));
    }

    @Override
    public CompletableFuture<Boolean> connect(UUID player, String target) {
      Optional<Player> p = server.getPlayer(player);
      Optional<RegisteredServer> rs = server.getServer(target);
      if (p.isEmpty() || rs.isEmpty()) {
        return CompletableFuture.completedFuture(false);
      }
      return p.get().createConnectionRequest(rs.get()).connect().thenApply(r -> r.isSuccessful());
    }

    @Override
    public CompletableFuture<Boolean> disconnect(UUID player, Component reason) {
      return CompletableFuture.completedFuture(server.getPlayer(player).map(p -> {
        p.disconnect(reason);
        return true;
      }).orElse(false));
    }
  }
}
