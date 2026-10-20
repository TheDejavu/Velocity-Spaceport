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

package com.velocitypowered.proxy.connection.backend;

import com.velocitypowered.api.network.NetworkPlayer;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.util.UuidUtils;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.connection.MinecraftConnection;
import com.velocitypowered.proxy.connection.client.ConnectedPlayer;
import com.velocitypowered.proxy.network.cluster.NetworkActions;
import com.velocitypowered.proxy.network.cluster.VelocityNetwork;
import com.velocitypowered.proxy.network.cluster.VelocityNetworkPlayer;
import com.velocitypowered.proxy.protocol.packet.PluginMessagePacket;
import com.velocitypowered.proxy.protocol.util.ByteBufDataInput;
import com.velocitypowered.proxy.protocol.util.ByteBufDataOutput;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.ComponentSerializer;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import net.kyori.adventure.text.serializer.legacy.LegacyComponentSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: answers the BungeeCord channel for the whole network when the network is enabled.
 * Answers that need other proxies arrive later; they go back only to the backend connection the
 * question came from, and are dropped if the player has moved on since.
 */
@SuppressFBWarnings(value = "OS_OPEN_STREAM", justification = "heap ByteBuf streams")
final class NetworkBungeeBridge {

  private final VelocityServer proxy;
  private final VelocityNetwork network;
  private final NetworkActions actions;
  private final ConnectedPlayer player;
  private final @Nullable VelocityServerConnection origin;

  NetworkBungeeBridge(VelocityServer proxy, NetworkActions actions, ConnectedPlayer player) {
    this.proxy = proxy;
    this.network = proxy.getNetwork();
    this.actions = actions;
    this.player = player;
    this.origin = player.getConnectedServer();
  }

  /**
   * Handles a subchannel that has a network-wide meaning.
   *
   * @param subChannel the subchannel
   * @param in the rest of the message
   * @return false if the local handling applies
   */
  boolean process(String subChannel, ByteBufDataInput in) {
    switch (subChannel) {
      case "PlayerCount" -> playerCount(in.readUTF());
      case "PlayerList" -> playerList(in.readUTF());
      case "GetPlayerServer" -> getPlayerServer(in.readUTF());
      case "ConnectOther" -> {
        String name = in.readUTF();
        String server = in.readUTF();
        withPlayer(name, p -> p.connect(server));
      }
      case "Message" -> message(in, LegacyComponentSerializer.legacySection());
      case "MessageRaw" -> message(in, GsonComponentSerializer.gson());
      case "KickPlayer" -> kick(in, LegacyComponentSerializer.legacySection());
      case "KickPlayerRaw" -> kick(in, GsonComponentSerializer.gson());
      case "UUIDOther" -> uuidOther(in.readUTF());
      case "IPOther" -> ipOther(in.readUTF());
      case "ForwardToPlayer" -> {
        String name = in.readUTF();
        byte[] data = rest(in);
        withPlayer(name, p -> actions.forwardToPlayer(p.getUniqueId(), data));
      }
      case "Forward" -> {
        String target = in.readUTF();
        byte[] data = rest(in);
        if (target.equals("ALL") || target.equals("ONLINE")) {
          actions.forwardToServers(null,
              origin == null ? null : origin.getServerInfo().getName(), data);
        } else {
          actions.forwardToServers(target, null, data);
        }
      }
      default -> {
        return false;
      }
    }
    return true;
  }

  private static byte[] rest(ByteBufDataInput in) {
    ByteBuf buf = in.unwrap();
    byte[] data = new byte[buf.readableBytes()];
    buf.readBytes(data);
    return data;
  }

  private void withPlayer(String name, Consumer<NetworkPlayer> action) {
    network.getPlayer(name).thenAccept(p -> p.ifPresent(action));
  }

  private void playerCount(String target) {
    if (target.equals("ALL")) {
      reply(CompletableFuture.completedFuture(response(out -> {
        out.writeUTF("PlayerCount");
        out.writeUTF("ALL");
        out.writeInt(network.getPlayerCount());
      })));
      return;
    }
    reply(network.getPlayers().thenApply(all -> {
      int n = (int) all.stream().filter(p -> onServer(p, target)).count();
      return response(out -> {
        out.writeUTF("PlayerCount");
        out.writeUTF(target);
        out.writeInt(n);
      });
    }));
  }

  private void playerList(String target) {
    reply(network.getPlayers().thenApply(all -> {
      StringJoiner names = new StringJoiner(", ");
      all.stream().filter(p -> target.equals("ALL") || onServer(p, target))
          .forEach(p -> names.add(p.getUsername()));
      return response(out -> {
        out.writeUTF("PlayerList");
        out.writeUTF(target);
        out.writeUTF(names.toString());
      });
    }));
  }

  private static boolean onServer(NetworkPlayer p, String server) {
    return p.getServerName().map(server::equalsIgnoreCase).orElse(false);
  }

  private void getPlayerServer(String name) {
    reply(network.getPlayer(name).thenApply(p -> p.flatMap(np -> np.getServerName()
        .map(server -> response(out -> {
          out.writeUTF("GetPlayerServer");
          out.writeUTF(np.getUsername());
          out.writeUTF(server);
        }))).orElse(null)));
  }

  private void uuidOther(String name) {
    reply(network.getPlayer(name).thenApply(p -> p.map(np -> response(out -> {
      out.writeUTF("UUIDOther");
      out.writeUTF(np.getUsername());
      out.writeUTF(UuidUtils.toUndashed(np.getUniqueId()));
    })).orElse(null)));
  }

  private void ipOther(String name) {
    reply(network.getPlayer(name).thenApply(p -> p.map(np -> {
      // The port is only known on the proxy the player is connected to.
      Optional<Player> local = proxy.getPlayer(np.getUniqueId());
      String ip = local.map(x -> x.getRemoteAddress().getHostString())
          .orElse(((VelocityNetworkPlayer) np).record().ip());
      int port = local.map(x -> x.getRemoteAddress().getPort()).orElse(0);
      return response(out -> {
        out.writeUTF("IPOther");
        out.writeUTF(np.getUsername());
        out.writeUTF(ip);
        out.writeInt(port);
      });
    }).orElse(null)));
  }

  private void message(ByteBufDataInput in, ComponentSerializer<Component, ?, String> serializer) {
    String target = in.readUTF();
    Component message = serializer.deserialize(in.readUTF());
    if (target.equals("ALL")) {
      network.broadcast(message);
    } else {
      withPlayer(target, p -> p.sendMessage(message));
    }
  }

  private void kick(ByteBufDataInput in, ComponentSerializer<Component, ?, String> serializer) {
    String name = in.readUTF();
    Component reason = serializer.deserialize(in.readUTF());
    withPlayer(name, p -> p.disconnect(reason));
  }

  @FunctionalInterface
  private interface Writer {
    void write(ByteBufDataOutput out);
  }

  private static ByteBuf response(Writer writer) {
    ByteBuf buf = Unpooled.buffer();
    writer.write(new ByteBufDataOutput(buf));
    return buf;
  }

  /** Sends an answer back on the connection the question came from, if the player is still on it. */
  private void reply(CompletableFuture<@Nullable ByteBuf> answer) {
    answer.whenComplete((buf, ex) -> {
      if (buf == null) {
        return;
      }
      MinecraftConnection connection = origin == null ? null : origin.getConnection();
      if (connection == null || player.getConnectedServer() != origin) {
        buf.release(); // the player moved on; the answer is for a server it has left
        return;
      }
      connection.write(new PluginMessagePacket(BungeeCordMessageResponder
          .getBungeeCordChannel(connection.getProtocolVersion()).getId(), buf));
    });
  }
}
