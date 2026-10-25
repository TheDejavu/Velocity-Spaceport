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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.velocitypowered.api.network.LastSeen;
import com.velocitypowered.api.network.Network;
import com.velocitypowered.api.network.NetworkPlayer;
import com.velocitypowered.api.network.ProxyNode;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.ServerConnection;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.api.util.GameProfile;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.kyori.adventure.text.Component;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link Network} for the API: the cluster when the network is enabled, this proxy alone when not.
 */
public final class VelocityNetwork implements Network {

  private record Node(String getId, int getPlayerCount, boolean isDraining) implements ProxyNode {
  }

  private final VelocityServer server;
  private final @Nullable ClusterService cluster;
  private final @Nullable NetworkActions remote;
  private final VelocityNetworkPlayer.Actions actions;
  private final PluginMessenger messenger;
  /** Players that left, kept in memory when there is no network (bounded). */
  private final Cache<UUID, LastSeen> seen = Caffeine.newBuilder().maximumSize(50_000).build();
  private final Cache<String, UUID> seenNames = Caffeine.newBuilder().maximumSize(50_000).build();

  /**
   * Creates the network view.
   *
   * @param server the proxy
   * @param cluster the cluster, or null when the network is off
   * @param remote the cluster's actions, or null when the network is off
   */
  public VelocityNetwork(VelocityServer server, @Nullable ClusterService cluster,
      @Nullable NetworkActions remote) {
    this(server, cluster, remote, new PluginMessenger(server, cluster));
  }

  /**
   * Creates the network view with a messenger that was created before the cluster started, so no
   * plugin message can arrive before it listens.
   *
   * @param server the proxy
   * @param cluster the cluster, or null when the network is off
   * @param remote the cluster's actions, or null when the network is off
   * @param messenger the plugin messenger, on the same cluster
   */
  public VelocityNetwork(VelocityServer server, @Nullable ClusterService cluster,
      @Nullable NetworkActions remote, PluginMessenger messenger) {
    this.messenger = messenger;
    this.server = server;
    this.cluster = cluster;
    this.remote = remote;
    this.actions = remote != null ? remote : new LocalActions();
  }

  @Override
  public PluginMessenger getMessenger() {
    return messenger;
  }

  /** Stops the plugin message threads, on shutdown. */
  public void close() {
    messenger.close();
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
  public int getPlayerCount(String serverName) {
    int local = server.getServer(serverName).map(RegisteredServer::getPlayersConnected)
        .map(Collection::size).orElse(0);
    return cluster == null ? local : local + cluster.remotePlayerCount(serverName);
  }

  @Override
  public Collection<ProxyNode> getProxies() {
    List<ProxyNode> nodes = new ArrayList<>();
    nodes.add(new Node(getProxyId(), server.getPlayerCount(), server.getHealth().isDraining()));
    if (cluster != null) {
      for (ProxyState p : cluster.liveRemotes()) {
        nodes.add(new Node(p.id(), p.players(), p.draining()));
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
  public CompletableFuture<Collection<NetworkPlayer>> getPlayers(String serverName) {
    if (remote == null) {
      List<NetworkPlayer> players = new ArrayList<>();
      server.getServer(serverName).ifPresent(rs -> rs.getPlayersConnected()
          .forEach(p -> players.add(local(p))));
      return CompletableFuture.completedFuture(players);
    }
    return remote.playersOnServer(serverName).thenApply(this::wrapAll);
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

  @Override
  public boolean isDraining() {
    return server.getHealth().isDraining();
  }

  @Override
  public CompletableFuture<Void> drain() {
    return server.getHealth().drain();
  }

  @Override
  public void cancelDrain() {
    server.getHealth().cancelDrain();
  }

  @Override
  public CompletableFuture<Optional<GameProfile>> getCachedProfile(UUID uuid) {
    return cluster == null ? CompletableFuture.completedFuture(Optional.empty())
        : cluster.store().profile(uuid).thenApply(p -> p.map(VelocityNetwork::toProfile));
  }

  @Override
  public CompletableFuture<Optional<GameProfile>> getCachedProfile(String username) {
    return cluster == null ? CompletableFuture.completedFuture(Optional.empty())
        : cluster.store().profileByName(username).thenApply(p -> p.map(VelocityNetwork::toProfile));
  }

  @Override
  public int getPlayerCountOnProxy(String proxyId) {
    return getProxy(proxyId).map(ProxyNode::getPlayerCount).orElse(0);
  }

  @Override
  public Optional<ProxyNode> getProxy(String proxyId) {
    return getProxies().stream().filter(p -> p.getId().equals(proxyId)).findFirst();
  }

  @Override
  public CompletableFuture<Collection<NetworkPlayer>> getPlayersOnProxy(String proxyId) {
    if (remote == null) {
      return proxyId.equals(getProxyId()) ? getPlayers()
          : CompletableFuture.completedFuture(List.of());
    }
    return remote.playersOnProxy(proxyId).thenApply(this::wrapAll);
  }

  private Collection<NetworkPlayer> wrapAll(List<PlayerRecord> records) {
    List<NetworkPlayer> players = new ArrayList<>(records.size());
    records.forEach(r -> players.add(wrap(r)));
    return players;
  }

  @Override
  public CompletableFuture<Boolean> isOnline(UUID uuid) {
    if (server.getPlayer(uuid).isPresent()) {
      return CompletableFuture.completedFuture(true);
    }
    return cluster == null ? CompletableFuture.completedFuture(false)
        : cluster.store().player(uuid).thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Boolean> isOnline(String username) {
    if (server.getPlayer(username).isPresent()) {
      return CompletableFuture.completedFuture(true);
    }
    return cluster == null ? CompletableFuture.completedFuture(false)
        : cluster.store().uuidOf(username).thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Optional<UUID>> getUniqueId(String username) {
    Optional<Player> online = server.getPlayer(username);
    if (online.isPresent()) {
      return CompletableFuture.completedFuture(online.map(Player::getUniqueId));
    }
    if (cluster == null) {
      return CompletableFuture.completedFuture(Optional.ofNullable(
          seenNames.getIfPresent(username.toLowerCase(Locale.ROOT))));
    }
    NetworkStore store = cluster.store();
    return store.uuidOf(username).thenCompose(id -> id.isPresent()
        ? CompletableFuture.completedFuture(id)
        : store.profileByName(username).thenCompose(p -> p.isPresent()
            ? CompletableFuture.completedFuture(p.map(NetworkStore.CachedProfile::uuid))
            : cluster.seenUuid(username)));
  }

  @Override
  public CompletableFuture<Optional<String>> getUsername(UUID uuid) {
    Optional<Player> online = server.getPlayer(uuid);
    if (online.isPresent()) {
      return CompletableFuture.completedFuture(online.map(Player::getUsername));
    }
    if (cluster == null) {
      return CompletableFuture.completedFuture(Optional.ofNullable(seen.getIfPresent(uuid))
          .map(LastSeen::getUsername));
    }
    NetworkStore store = cluster.store();
    return store.player(uuid).thenCompose(r -> r.isPresent()
        ? CompletableFuture.completedFuture(r.map(PlayerRecord::name))
        : store.profile(uuid).thenCompose(p -> p.isPresent()
            ? CompletableFuture.completedFuture(p.map(NetworkStore.CachedProfile::name))
            : cluster.lastSeen(uuid).thenApply(l -> l.map(LastSeen::getUsername))));
  }

  @Override
  public CompletableFuture<Optional<LastSeen>> getLastSeen(UUID uuid) {
    Optional<Player> online = server.getPlayer(uuid);
    if (online.isPresent()) {
      NetworkPlayer p = local(online.get());
      return CompletableFuture.completedFuture(Optional.of(now(p)));
    }
    if (cluster == null) {
      LastSeen last = seen.getIfPresent(uuid);
      long ttl = seenTtlMillis();
      return CompletableFuture.completedFuture(last == null || ttl <= 0
          || last.getTime().toEpochMilli() + ttl < System.currentTimeMillis()
          ? Optional.empty() : Optional.of(last));
    }
    return cluster.store().player(uuid).thenCompose(r -> r.isPresent()
        ? CompletableFuture.completedFuture(Optional.of(now(wrap(r.get()))))
        : cluster.lastSeen(uuid));
  }

  @Override
  public CompletableFuture<Optional<LastSeen>> getLastSeen(String username) {
    return getUniqueId(username).thenCompose(id -> id.isEmpty()
        ? CompletableFuture.completedFuture(Optional.<LastSeen>empty()) : getLastSeen(id.get()));
  }

  @Override
  public CompletableFuture<Optional<String>> getLastServer(UUID uuid) {
    return getLastSeen(uuid).thenApply(l -> l.flatMap(LastSeen::getServerName));
  }

  private static LastSeen now(NetworkPlayer p) {
    return new LastSeen(p.getUniqueId(), p.getUsername(), p.getProxyId(),
        p.getServerName().orElse(null), Instant.now(), true);
  }

  private long seenTtlMillis() {
    return server.getConfiguration().getNetworkConfig().lastSeenTtl() * 86_400_000L;
  }

  /**
   * Remembers a local player that left, when there is no network to keep the record.
   *
   * @param player the player
   */
  public void recordQuit(Player player) {
    if (cluster != null || seenTtlMillis() <= 0) {
      return;
    }
    LastSeen last = new LastSeen(player.getUniqueId(), player.getUsername(), getProxyId(),
        player.getCurrentServer().map(c -> c.getServerInfo().getName()).orElse(null),
        Instant.now(), false);
    seen.put(player.getUniqueId(), last);
    seenNames.put(player.getUsername().toLowerCase(Locale.ROOT), player.getUniqueId());
  }

  private static GameProfile toProfile(NetworkStore.CachedProfile p) {
    return new GameProfile(p.uuid(), p.name(), p.properties() == null ? List.of()
        : List.of(VelocityServer.GENERAL_GSON.fromJson(p.properties(),
            GameProfile.Property[].class)));
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
