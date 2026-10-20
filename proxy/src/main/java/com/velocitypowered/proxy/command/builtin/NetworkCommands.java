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
import com.velocitypowered.api.proxy.ProxyServer;
import com.velocitypowered.proxy.plugin.virtual.VelocityVirtualPlugin;
import java.util.List;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.minimessage.MiniMessage;

/**
 * Spaceport: {@code /find}, {@code /alert} and {@code /proxycmd}, registered when the network is
 * enabled.
 */
public final class NetworkCommands {

  private NetworkCommands() {
  }

  /**
   * Registers the commands.
   *
   * @param server the proxy
   */
  public static void register(ProxyServer server) {
    Network network = server.getNetwork();
    register(server, LiteralArgumentBuilder.<CommandSource>literal("find")
        .requires(s -> s.getPermissionValue("velocity.command.find") == Tristate.TRUE)
        .then(RequiredArgumentBuilder.<CommandSource, String>argument("player",
                StringArgumentType.word())
            .executes(context -> {
              CommandSource source = context.getSource();
              String name = context.getArgument("player", String.class);
              network.getPlayer(name).thenAccept(p -> source.sendMessage(p.map(np ->
                  Component.text(np.getUsername() + " is on proxy " + np.getProxyId()
                      + np.getServerName().map(s -> ", server " + s).orElse(""),
                      NamedTextColor.YELLOW))
                  .orElse(Component.text(name + " is not online.", NamedTextColor.RED))));
              return Command.SINGLE_SUCCESS;
            })));
    register(server, LiteralArgumentBuilder.<CommandSource>literal("alert")
        .requires(s -> s.getPermissionValue("velocity.command.alert") == Tristate.TRUE)
        .then(RequiredArgumentBuilder.<CommandSource, String>argument("message",
                StringArgumentType.greedyString())
            .executes(context -> {
              network.broadcast(MiniMessage.miniMessage()
                  .deserialize(context.getArgument("message", String.class)));
              return Command.SINGLE_SUCCESS;
            })));
    register(server, LiteralArgumentBuilder.<CommandSource>literal("proxycmd")
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
                      source.sendMessage(Component.text("[" + proxy + "] " + (ex != null
                          ? "no answer" : ok ? "done" : "failed"),
                          ex == null && ok ? NamedTextColor.GREEN : NamedTextColor.RED));
                      return null;
                    });
                  }
                  return Command.SINGLE_SUCCESS;
                }))));
  }

  private static void register(ProxyServer server,
      LiteralArgumentBuilder<CommandSource> node) {
    BrigadierCommand command = new BrigadierCommand(node);
    server.getCommandManager().register(server.getCommandManager().metaBuilder(command)
        .plugin(VelocityVirtualPlugin.INSTANCE).build(), command);
  }
}
