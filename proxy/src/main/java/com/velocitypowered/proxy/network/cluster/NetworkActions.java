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

import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.backend.BungeeCordMessageResponder;
import com.velocitypowered.proxy.connection.backend.VelocityServerConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.network.cluster.MessageBus.Reply;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Actions on players and proxies anywhere on the network: run here for local players, sent over
 * the {@link MessageBus} to the owning proxy otherwise. A player that moved to another proxy
 * while a request was on its way is looked up again and followed.
 */
public final class NetworkActions implements VelocityNetworkPlayer.Actions {

  static final byte MESSAGE = 1;
  static final byte CONNECT = 2;
  static final byte KICK = 3;
  static final byte COMMAND = 4;
  static final byte LIST = 5;
  static final byte BROADCAST = 6;
  static final byte FORWARD_SERVERS = 7;
  static final byte FORWARD_PLAYER = 8;

  private static final GsonComponentSerializer JSON = GsonComponentSerializer.gson();

  private final VelocityServer server;
  private final ClusterService cluster;

  /**
   * Registers the request handlers of this proxy.
   *
   * @param server the proxy
   * @param cluster its network membership
   */
  public NetworkActions(VelocityServer server, ClusterService cluster) {
    this.server = server;
    this.cluster = cluster;
    MessageBus bus = cluster.bus();
    bus.register(MESSAGE, onPlayer(in -> {
      Component message = JSON.deserialize(in.readUTF());
      return p -> {
        p.sendMessage(message);
        return CompletableFuture.completedFuture(true);
      };
    }));
    bus.register(CONNECT, onPlayer(in -> {
      String target = in.readUTF();
      return p -> connectLocal(p, target);
    }));
    bus.register(KICK, onPlayer(in -> {
      Component reason = JSON.deserialize(in.readUTF());
      return p -> {
        p.disconnect(reason);
        return CompletableFuture.completedFuture(true);
      };
    }));
    bus.register(FORWARD_PLAYER, onPlayer(in -> {
      byte[] data = in.readNBytes(in.readInt());
      return p -> CompletableFuture.completedFuture(
          BungeeCordMessageResponder.forwardToPlayer(p, data));
    }));
    bus.register(COMMAND, (from, in) -> {
      String command = in.readUTF();
      return server.getCommandManager().executeAsync(server.getConsoleCommandSource(), command)
          .thenApply(ok -> Reply.of(ok ? MessageBus.OK : MessageBus.FAILED));
    });
    bus.register(BROADCAST, (from, in) -> {
      server.sendMessage(JSON.deserialize(in.readUTF()));
      return CompletableFuture.completedFuture(Reply.of(MessageBus.OK));
    });
    bus.register(LIST, (from, in) -> CompletableFuture.completedFuture(
        new Reply(MessageBus.OK, encodePlayers(localPlayers()))));
    bus.register(FORWARD_SERVERS, (from, in) -> {
      int n = in.readInt();
      List<String> servers = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        servers.add(in.readUTF());
      }
      byte[] data = in.readNBytes(in.readInt());
      for (String name : servers) {
        server.getServer(name).ifPresent(rs -> BungeeCordMessageResponder.forwardToServer(rs,
            data));
      }
      return CompletableFuture.completedFuture(Reply.of(MessageBus.OK));
    });
  }

  /** Parses a request about one player and runs it if the player is here. */
  private MessageBus.Handler onPlayer(Parser parser) {
    return (from, in) -> {
      UUID uuid = new UUID(in.readLong(), in.readLong());
      Function<ConnectedPlayer, CompletableFuture<Boolean>> action = parser.parse(in);
      Optional<ConnectedPlayer> player = local(uuid);
      if (player.isEmpty()) {
        return CompletableFuture.completedFuture(Reply.of(MessageBus.NOT_HERE));
      }
      return action.apply(player.get())
          .thenApply(ok -> Reply.of(ok ? MessageBus.OK : MessageBus.FAILED));
    };
  }

  @FunctionalInterface
  private interface Parser {
    Function<ConnectedPlayer, CompletableFuture<Boolean>> parse(DataInputStream in)
        throws IOException;
  }

  private Optional<ConnectedPlayer> local(UUID uuid) {
    return server.getPlayer(uuid).map(p -> (ConnectedPlayer) p);
  }

  private CompletableFuture<Boolean> connectLocal(ConnectedPlayer player, String target) {
    Optional<RegisteredServer> rs = server.getServer(target);
    if (rs.isEmpty()) {
      return CompletableFuture.completedFuture(false);
    }
    return player.createConnectionRequest(rs.get()).connect()
        .thenApply(r -> r.isSuccessful());
  }

  /**
   * Runs an action on a player: here if it is local, else on the proxy that owns it. A player
   * that moved while the request was on its way is followed (twice at most).
   */
  private CompletableFuture<Boolean> runOnPlayer(UUID uuid, byte action,
      MessageBus.Writer fields,
      Function<ConnectedPlayer, CompletableFuture<Boolean>> localAction, int hops) {
    Optional<ConnectedPlayer> player = local(uuid);
    if (player.isPresent()) {
      return localAction.apply(player.get());
    }
    return cluster.store().player(uuid).thenCompose(record -> {
      if (record.isEmpty() || record.get().proxy().equals(cluster.proxyId()) || hops > 2) {
        return CompletableFuture.completedFuture(false);
      }
      byte[] payload = MessageBus.encode(out -> {
        out.writeLong(uuid.getMostSignificantBits());
        out.writeLong(uuid.getLeastSignificantBits());
        fields.write(out);
      });
      return cluster.bus().request(record.get().proxy(), action, payload).thenCompose(reply ->
          reply.status() == MessageBus.NOT_HERE
              ? runOnPlayer(uuid, action, fields, localAction, hops + 1)
              : CompletableFuture.completedFuture(reply.ok()));
    });
  }

  @Override
  public CompletableFuture<Boolean> message(UUID player, Component message) {
    String json = JSON.serialize(message);
    return runOnPlayer(player, MESSAGE, out -> out.writeUTF(json), p -> {
      p.sendMessage(message);
      return CompletableFuture.completedFuture(true);
    }, 0);
  }

  @Override
  public CompletableFuture<Boolean> connect(UUID player, String target) {
    return runOnPlayer(player, CONNECT, out -> out.writeUTF(target),
        p -> connectLocal(p, target), 0);
  }

  @Override
  public CompletableFuture<Boolean> disconnect(UUID player, Component reason) {
    String json = JSON.serialize(reason);
    return runOnPlayer(player, KICK, out -> out.writeUTF(json), p -> {
      p.disconnect(reason);
      return CompletableFuture.completedFuture(true);
    }, 0);
  }

  /**
   * Sends a BungeeCord channel message to the backend server of a player anywhere on the network.
   *
   * @param player the player
   * @param data the message, as ForwardToPlayer carries it
   * @return true if delivered to a backend
   */
  public CompletableFuture<Boolean> forwardToPlayer(UUID player, byte[] data) {
    return runOnPlayer(player, FORWARD_PLAYER, out -> {
      out.writeInt(data.length);
      out.write(data);
    }, p -> CompletableFuture.completedFuture(
        BungeeCordMessageResponder.forwardToPlayer(p, data)), 0);
  }

  /**
   * Lists every player on the network: this proxy's directly, the others' by asking them. A proxy
   * that does not answer in time is left out.
   *
   * @return the players
   */
  public CompletableFuture<List<PlayerRecord>> players() {
    List<CompletableFuture<List<PlayerRecord>>> parts = new ArrayList<>();
    parts.add(CompletableFuture.completedFuture(localPlayers()));
    for (ProxyState proxy : cluster.liveRemotes()) {
      parts.add(cluster.bus().request(proxy.id(), LIST, new byte[0])
          .thenApply(reply -> reply.ok() ? decodePlayers(proxy.id(), reply.payload())
              : List.<PlayerRecord>of())
          .exceptionally(ex -> List.of()));
    }
    return CompletableFuture.allOf(parts.toArray(CompletableFuture[]::new)).thenApply(v -> {
      List<PlayerRecord> all = new ArrayList<>();
      parts.forEach(p -> all.addAll(p.join()));
      return all;
    });
  }

  /**
   * Sends a chat message to every player on the network.
   *
   * @param message the message
   * @return completes when every live proxy answered or gave up
   */
  public CompletableFuture<Void> broadcast(Component message) {
    server.sendMessage(message);
    byte[] payload = MessageBus.encode(out -> out.writeUTF(JSON.serialize(message)));
    return CompletableFuture.allOf(cluster.liveRemotes().stream()
        .map(p -> cluster.bus().request(p.id(), BROADCAST, payload).exceptionally(ex -> null))
        .toArray(CompletableFuture[]::new));
  }

  /**
   * Runs a console command on a proxy of the network.
   *
   * @param proxyId the proxy
   * @param command the command line
   * @return true if it ran and succeeded
   */
  public CompletableFuture<Boolean> command(String proxyId, String command) {
    if (proxyId.equals(cluster.proxyId())) {
      return server.getCommandManager().executeAsync(server.getConsoleCommandSource(), command);
    }
    return cluster.bus().request(proxyId, COMMAND, MessageBus.encode(out -> out.writeUTF(command)))
        .thenApply(Reply::ok);
  }

  /**
   * BungeeCord Forward across the network, each backend server getting it once: servers with
   * players on this proxy get it from here; every other server from one proxy that has players
   * on it (the one with the lowest id), as seen in the last tick.
   *
   * @param only the one server to send to, or null for every server
   * @param except a server to skip (the sender's, for Forward ALL), or null
   * @param data the message
   */
  public void forwardToServers(@Nullable String only, @Nullable String except, byte[] data) {
    Set<String> here = new HashSet<>(localServers());
    Map<String, List<String>> byProxy = new HashMap<>();
    Map<String, String> deliverer = new HashMap<>();
    // Non-draining proxies first, then the lowest id.
    List<ProxyState> remotes = new ArrayList<>(cluster.liveRemotes());
    remotes.sort(Comparator.comparing(ProxyState::draining).thenComparing(ProxyState::id));
    for (ProxyState proxy : remotes) {
      for (String name : proxy.servers()) {
        deliverer.putIfAbsent(name, proxy.id());
      }
    }
    Collection<RegisteredServer> targets = only == null ? server.getAllServers()
        : server.getServer(only).map(List::of).orElse(List.of());
    for (RegisteredServer rs : targets) {
      String name = rs.getServerInfo().getName();
      if (name.equals(except)) {
        continue;
      }
      if (here.contains(name)) {
        BungeeCordMessageResponder.forwardToServer(rs, data);
      } else if (deliverer.containsKey(name)) {
        byProxy.computeIfAbsent(deliverer.get(name), k -> new ArrayList<>()).add(name);
      }
    }
    byProxy.forEach((proxy, names) -> cluster.bus().request(proxy, FORWARD_SERVERS,
        MessageBus.encode(out -> {
          out.writeInt(names.size());
          for (String name : names) {
            out.writeUTF(name);
          }
          out.writeInt(data.length);
          out.write(data);
        })).exceptionally(ex -> null));
  }

  /**
   * Returns the servers this proxy has players on.
   *
   * @return the server names
   */
  public Set<String> localServers() {
    Set<String> names = new HashSet<>();
    for (var p : server.getAllPlayers()) {
      VelocityServerConnection connection = ((ConnectedPlayer) p).getConnectedServer();
      if (connection != null) {
        names.add(connection.getServerInfo().getName());
      }
    }
    return names;
  }

  private List<PlayerRecord> localPlayers() {
    List<PlayerRecord> players = new ArrayList<>();
    for (var p : server.getAllPlayers()) {
      ConnectedPlayer player = (ConnectedPlayer) p;
      VelocityServerConnection connection = player.getConnectedServer();
      long since = cluster.session(player.getUniqueId()).map(s -> s.record.since()).orElse(0L);
      players.add(new PlayerRecord(player.getUniqueId(), player.getUsername(), cluster.proxyId(),
          0, connection == null ? null : connection.getServerInfo().getName(),
          player.getRemoteAddress().getAddress().getHostAddress(), since));
    }
    return players;
  }

  private static byte[] encodePlayers(List<PlayerRecord> players) {
    return MessageBus.encode(out -> {
      out.writeInt(players.size());
      for (PlayerRecord p : players) {
        out.writeLong(p.uuid().getMostSignificantBits());
        out.writeLong(p.uuid().getLeastSignificantBits());
        out.writeUTF(p.name());
        out.writeUTF(p.server() == null ? "" : p.server());
        out.writeUTF(p.ip());
        out.writeLong(p.since());
      }
    });
  }

  private static List<PlayerRecord> decodePlayers(String proxy, byte[] payload) {
    try {
      DataInputStream in = MessageBus.decode(payload);
      int n = in.readInt();
      List<PlayerRecord> players = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        UUID uuid = new UUID(in.readLong(), in.readLong());
        String name = in.readUTF();
        String srv = in.readUTF();
        String ip = in.readUTF();
        players.add(new PlayerRecord(uuid, name, proxy, 0, srv.isEmpty() ? null : srv, ip,
            in.readLong()));
      }
      return players;
    } catch (IOException e) {
      return List.of();
    }
  }
}
