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

package com.velocitypowered.proxy.command.builtin;

import com.mojang.brigadier.Command;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.builder.LiteralArgumentBuilder;
import com.mojang.brigadier.builder.RequiredArgumentBuilder;
import com.velocitypowered.api.command.BrigadierCommand;
import com.velocitypowered.api.command.CommandSource;
import com.velocitypowered.api.network.Network;
import com.velocitypowered.api.network.ProxyNode;
import com.velocitypowered.api.permission.Tristate;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.plugin.virtual.VelocityVirtualPlugin;
import java.util.List;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.minimessage.MiniMessage;

/**
 * Spaceport: {@code /find}, {@code /alert}, {@code /proxycmd} (with the network on) and
 * {@code /spaceport drain|network}.
 */
public final class NetworkCommands {

  private NetworkCommands() {
  }

  private static Component text(String value) {
    return Component.text(value);
  }

  /**
   * Registers {@code /find}, {@code /alert} and {@code /proxycmd}.
   *
   * @param server the proxy
   */
  public static void register(VelocityServer server) {
    Network network = server.getNetwork();
    registerNode(server, LiteralArgumentBuilder.<CommandSource>literal("find")
        .requires(s -> s.getPermissionValue("velocity.command.find") == Tristate.TRUE)
        .then(RequiredArgumentBuilder.<CommandSource, String>argument("player",
                StringArgumentType.word())
            .executes(context -> {
              CommandSource source = context.getSource();
              String name = context.getArgument("player", String.class);
              network.getPlayer(name).thenAccept(p -> source.sendMessage(p.map(np ->
                  np.getServerName().map(s -> Component.translatable(
                          "velocity.command.find-found-server", NamedTextColor.YELLOW,
                          text(np.getUsername()), text(np.getProxyId()), text(s)))
                      .orElse(Component.translatable("velocity.command.find-found",
                          NamedTextColor.YELLOW, text(np.getUsername()), text(np.getProxyId()))))
                  .orElse(Component.translatable("velocity.command.find-not-online",
                      NamedTextColor.RED, text(name)))));
              return Command.SINGLE_SUCCESS;
            })));
    registerNode(server, LiteralArgumentBuilder.<CommandSource>literal("alert")
        .requires(s -> s.getPermissionValue("velocity.command.alert") == Tristate.TRUE)
        .then(RequiredArgumentBuilder.<CommandSource, String>argument("message",
                StringArgumentType.greedyString())
            .executes(context -> {
              network.broadcast(MiniMessage.miniMessage()
                  .deserialize(context.getArgument("message", String.class)));
              return Command.SINGLE_SUCCESS;
            })));
    registerNode(server, LiteralArgumentBuilder.<CommandSource>literal("proxycmd")
        .requires(s -> s.getPermissionValue("velocity.command.proxycmd") == Tristate.TRUE)
        .then(RequiredArgumentBuilder.<CommandSource, String>argument("proxy",
                StringArgumentType.word())
            .suggests((context, builder) -> {
              builder.suggest("all");
              network.getProxies().forEach(p -> builder.suggest(p.getId()));
              return builder.buildFuture();
            })
            .then(RequiredArgumentBuilder.<CommandSource, String>argument("command",
                    StringArgumentType.greedyString())
                .executes(context -> {
                  CommandSource source = context.getSource();
                  String target = context.getArgument("proxy", String.class);
                  String command = context.getArgument("command", String.class);
                  List<String> proxies = target.equals("all")
                      ? network.getProxies().stream().map(ProxyNode::getId).toList()
                      : List.of(target);
                  for (String proxy : proxies) {
                    network.executeCommand(proxy, command).handle((ok, ex) -> {
                      source.sendMessage(ex != null
                          ? Component.translatable("velocity.command.proxycmd-no-answer",
                              NamedTextColor.RED, text(proxy))
                          : ok ? Component.translatable("velocity.command.proxycmd-done",
                              NamedTextColor.GREEN, text(proxy))
                          : Component.translatable("velocity.command.proxycmd-failed",
                              NamedTextColor.RED, text(proxy)));
                      return null;
                    });
                  }
                  return Command.SINGLE_SUCCESS;
                }))));
  }

  /**
   * Registers {@code /spaceport drain [cancel]} and {@code /spaceport network}.
   *
   * @param server the proxy
   */
  public static void registerSpaceport(VelocityServer server) {
    Network network = server.getNetwork();
    registerNode(server, LiteralArgumentBuilder.<CommandSource>literal("spaceport")
        .requires(s -> s.getPermissionValue("velocity.command.spaceport") == Tristate.TRUE)
        .then(LiteralArgumentBuilder.<CommandSource>literal("drain")
            .executes(context -> {
              CommandSource source = context.getSource();
              source.sendMessage(Component.translatable("velocity.command.spaceport-drain-started",
                  NamedTextColor.YELLOW));
              network.drain().thenRun(() -> source.sendMessage(Component.translatable(
                  "velocity.command.spaceport-drain-marked", NamedTextColor.YELLOW)));
              return Command.SINGLE_SUCCESS;
            })
            .then(LiteralArgumentBuilder.<CommandSource>literal("cancel")
                .executes(context -> {
                  network.cancelDrain();
                  context.getSource().sendMessage(Component.translatable(
                      "velocity.command.spaceport-drain-cancelled", NamedTextColor.GREEN));
                  return Command.SINGLE_SUCCESS;
                })))
        .then(LiteralArgumentBuilder.<CommandSource>literal("auth-outage")
            .requires(s -> server.getCluster() != null)
            .then(LiteralArgumentBuilder.<CommandSource>literal("on").executes(context -> {
              server.getCluster().setAuthOutageMode(true);
              context.getSource().sendMessage(Component.translatable(
                  "velocity.command.spaceport-auth-outage-on", NamedTextColor.YELLOW));
              return Command.SINGLE_SUCCESS;
            }))
            .then(LiteralArgumentBuilder.<CommandSource>literal("off").executes(context -> {
              server.getCluster().setAuthOutageMode(false);
              context.getSource().sendMessage(Component.translatable(
                  "velocity.command.spaceport-auth-outage-off", NamedTextColor.GREEN));
              return Command.SINGLE_SUCCESS;
            })))
        .then(LiteralArgumentBuilder.<CommandSource>literal("network")
            .executes(context -> {
              CommandSource source = context.getSource();
              String problem = server.getHealth().readyProblem();
              source.sendMessage(Component.translatable("velocity.command.spaceport-network-self",
                  NamedTextColor.YELLOW, text(network.getProxyId()),
                  text(Integer.toString(network.getPlayerCount())),
                  text(problem == null ? "yes" : "no (" + problem + ")")));
              for (ProxyNode node : network.getProxies()) {
                source.sendMessage(Component.translatable(node.isDraining()
                        ? "velocity.command.spaceport-network-proxy-draining"
                        : "velocity.command.spaceport-network-proxy", NamedTextColor.GRAY,
                    text(node.getId()), text(Integer.toString(node.getPlayerCount()))));
              }
              return Command.SINGLE_SUCCESS;
            })));
  }

  private static void registerNode(VelocityServer server,
      LiteralArgumentBuilder<CommandSource> node) {
    BrigadierCommand command = new BrigadierCommand(node);
    server.getCommandManager().register(server.getCommandManager().metaBuilder(command)
        .plugin(VelocityVirtualPlugin.INSTANCE).build(), command);
  }
}
