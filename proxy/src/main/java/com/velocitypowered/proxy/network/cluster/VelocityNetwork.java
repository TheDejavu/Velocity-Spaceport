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
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link Network} for the API: the cluster when the network is enabled, this proxy alone when not.
 */
public final class VelocityNetwork implements Network {

  private record Node(String getId, int getPlayerCount) implements ProxyNode {
  }

  private final VelocityServer server;
  private final @Nullable ClusterService cluster;

  public VelocityNetwork(VelocityServer server, @Nullable ClusterService cluster) {
    this.server = server;
    this.cluster = cluster;
  }

  public @Nullable ClusterService cluster() {
    return cluster;
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
    if (cluster == null) {
      return List.of(new Node(getProxyId(), server.getPlayerCount()));
    }
    List<ProxyNode> nodes = new ArrayList<>();
    for (ProxyState p : cluster.proxies()) {
      if (p.id().equals(cluster.proxyId())) {
        nodes.add(new Node(p.id(), server.getPlayerCount()));
      } else if (p.players() >= 0 && p.ageMillis() < ClusterService.DEAD_AFTER_MILLIS) {
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
    return cluster.store().player(uuid).thenApply(r -> r.map(VelocityNetworkPlayer::new));
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

  private NetworkPlayer local(Player p) {
    long since = cluster == null ? 0 : cluster.session(p.getUniqueId())
        .map(s -> s.record.since()).orElse(0L);
    return new VelocityNetworkPlayer(new PlayerRecord(p.getUniqueId(), p.getUsername(),
        getProxyId(), 0, p.getCurrentServer().map(ServerConnection::getServerInfo)
        .map(i -> i.getName()).orElse(null), p.getRemoteAddress().getHostString(), since));
  }
}
