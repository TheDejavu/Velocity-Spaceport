/*
 * Copyright (C) 2020-2023 Velocity Contributors
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

import com.mojang.brigadier.StringReader;
import com.mojang.brigadier.arguments.ArgumentType;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import java.util.Collection;
import java.util.List;

/**
 * Spaceport: the first argument of {@code /send}: a player name, {@code all}, {@code current} or
 * {@code server:name}. Like a word, but {@code :} is allowed. Clients are told it is a plain word
 * (the command tree can only carry types they know); the proxy parses it with this type.
 */
public final class SendTargetArgumentType implements ArgumentType<String> {

  public static final SendTargetArgumentType INSTANCE = new SendTargetArgumentType();

  private SendTargetArgumentType() {
  }

  @Override
  public String parse(StringReader reader) throws CommandSyntaxException {
    if (reader.canRead() && StringReader.isQuotedStringStart(reader.peek())) {
      return reader.readQuotedString();
    }
    int start = reader.getCursor();
    while (reader.canRead() && reader.peek() != ' ') {
      reader.skip();
    }
    return reader.getString().substring(start, reader.getCursor());
  }

  @Override
  public Collection<String> getExamples() {
    return List.of("Steve", "all", "current", "server:lobby");
  }
}
