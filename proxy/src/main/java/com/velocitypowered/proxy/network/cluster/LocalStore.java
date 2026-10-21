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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * In-memory {@link NetworkStore} with the same rules as {@link RedisStore}. Several proxies in one
 * JVM (tests) can share an instance; {@code store = "local"} uses one for a single proxy.
 */
public final class LocalStore implements NetworkStore {

  private record ProxyEntry(int players, long seen, long expires, List<String> servers,
                            String instance, boolean draining) {
  }

  private record Owned(PlayerRecord record, long epoch) {
  }

  private final LongSupplier clock;
  private final Map<String, ProxyEntry> proxies = new HashMap<>();
  private final Map<String, Set<UUID>> members = new HashMap<>();
  private final Map<UUID, Owned> players = new HashMap<>();
  private final Map<String, UUID> names = new HashMap<>();
  private final Map<UUID, Expiring> routes = new HashMap<>();
  private final Map<String, Expiring> values = new HashMap<>();
  private final Map<UUID, Expiring> lastServers = new HashMap<>();
  private final Map<String, Long> nonces = new HashMap<>();
  private final Map<UUID, CachedProfile> profiles = new HashMap<>();
  private final Map<String, UUID> profileNames = new HashMap<>();
  private final Map<String, List<Consumer<byte[]>>> handlers = new HashMap<>();
  private String generation;
  private long epoch;
  private volatile boolean down;

  public LocalStore() {
    this(System::currentTimeMillis);
  }

  public LocalStore(LongSupplier clock) {
    this.clock = clock;
  }

  /**
   * For tests: while down, every call fails like a disconnected Redis.
   *
   * @param down whether the store is unreachable
   */
  public void setDown(boolean down) {
    this.down = down;
  }

  /** For tests: forget everything, like a Redis that lost its data. */
  public synchronized void wipe() {
    proxies.clear();
    members.clear();
    players.clear();
    names.clear();
    routes.clear();
    values.clear();
    lastServers.clear();
    nonces.clear();
    profiles.clear();
    profileNames.clear();
    generation = null;
    epoch = 0;
  }

  private <T> CompletableFuture<T> run(java.util.function.Supplier<T> op) {
    if (down) {
      return CompletableFuture.failedFuture(new IllegalStateException("store is down"));
    }
    synchronized (this) {
      return CompletableFuture.completedFuture(op.get());
    }
  }

  private ProxyEntry live(String id, long now) {
    ProxyEntry e = proxies.get(id);
    return e == null || e.expires() <= now ? null : e;
  }

  @Override
  public CompletableFuture<Tick> tick(String proxyId, int count, Collection<String> servers,
      String instance, boolean draining, long ttlMillis, String candidateGeneration) {
    return run(() -> {
      long now = clock.getAsLong();
      boolean created = generation == null;
      if (created) {
        generation = candidateGeneration;
      }
      final ProxyEntry before = live(proxyId, now);
      proxies.put(proxyId, new ProxyEntry(count, now, now + ttlMillis, List.copyOf(servers),
          instance, draining));
      members.computeIfAbsent(proxyId, k -> new HashSet<>());
      List<ProxyState> states = new ArrayList<>();
      for (String id : members.keySet()) {
        states.add(state(id, now).orElse(new ProxyState(id, -1, -1, List.of(), "", false)));
      }
      return new Tick(generation, created, before != null,
          before == null ? "" : before.instance(), states);
    });
  }

  private Optional<ProxyState> state(String id, long now) {
    ProxyEntry e = live(id, now);
    return e == null ? Optional.empty() : Optional.of(new ProxyState(id, e.players(),
        now - e.seen(), e.servers(), e.instance(), e.draining()));
  }

  @Override
  public CompletableFuture<Optional<ProxyState>> proxy(String proxyId) {
    return run(() -> state(proxyId, clock.getAsLong()));
  }

  @Override
  public CompletableFuture<Claim> claim(PlayerRecord p, long claimEpoch, long deadAfterMillis,
      boolean lastServer) {
    return run(() -> {
      long now = clock.getAsLong();
      Owned cur = players.get(p.uuid());
      String owner = cur == null ? null : cur.record().proxy();
      String crashed = null;
      if (owner != null && !owner.equals(p.proxy())) {
        ProxyEntry e = live(owner, now);
        if (e != null && now - e.seen() < deadAfterMillis) {
          return new Claim(false, owner);
        }
        members.getOrDefault(owner, new HashSet<>()).remove(p.uuid());
        crashed = lastServer ? cur.record().server() : null;
      }
      if (owner != null && owner.equals(p.proxy()) && cur.epoch() > claimEpoch) {
        return new Claim(false, owner);
      }
      players.put(p.uuid(), new Owned(p, claimEpoch));
      names.put(p.name().toLowerCase(Locale.ROOT), p.uuid());
      members.computeIfAbsent(p.proxy(), k -> new HashSet<>()).add(p.uuid());
      String route = take(routes, p.uuid(), now);
      String last = take(lastServers, p.uuid(), now);
      return new Claim(true, owner, route != null ? route : last != null ? last : crashed);
    });
  }

  private boolean owns(UUID player, String proxyId, long session) {
    Owned cur = players.get(player);
    return cur != null && cur.record().proxy().equals(proxyId)
        && cur.record().session() == session;
  }

  @Override
  public CompletableFuture<Boolean> updateServer(UUID player, String proxyId, long session,
      String server) {
    return run(() -> {
      if (!owns(player, proxyId, session)) {
        return false;
      }
      Owned cur = players.get(player);
      PlayerRecord r = cur.record();
      players.put(player, new Owned(new PlayerRecord(r.uuid(), r.name(), r.proxy(), r.session(),
          server, r.ip(), r.since()), cur.epoch()));
      return true;
    });
  }

  @Override
  public CompletableFuture<Boolean> release(UUID player, String proxyId, long session) {
    return run(() -> {
      if (!owns(player, proxyId, session)) {
        return false;
      }
      remove(player);
      members.getOrDefault(proxyId, new HashSet<>()).remove(player);
      return true;
    });
  }

  private void remove(UUID player) {
    Owned cur = players.remove(player);
    names.remove(cur.record().name().toLowerCase(Locale.ROOT), player);
  }

  @Override
  public CompletableFuture<Integer> sweep(String proxyId, long deadAfterMillis, boolean force,
      long lastServerTtlMillis) {
    return run(() -> {
      long now = clock.getAsLong();
      ProxyEntry e = live(proxyId, now);
      if (!force && e != null && now - e.seen() < deadAfterMillis) {
        return -1;
      }
      int n = 0;
      for (UUID uuid : members.getOrDefault(proxyId, Set.of())) {
        Owned cur = players.get(uuid);
        if (cur != null && cur.record().proxy().equals(proxyId)) {
          if (cur.record().server() != null && lastServerTtlMillis > 0) {
            lastServers.put(uuid, new Expiring(cur.record().server(), now + lastServerTtlMillis));
          }
          remove(uuid);
          n++;
        }
      }
      members.remove(proxyId);
      proxies.remove(proxyId);
      return n;
    });
  }

  @Override
  public CompletableFuture<Optional<PlayerRecord>> player(UUID player) {
    return run(() -> Optional.ofNullable(players.get(player)).map(Owned::record));
  }

  @Override
  public CompletableFuture<Optional<UUID>> uuidOf(String name) {
    return run(() -> Optional.ofNullable(names.get(name.toLowerCase(Locale.ROOT))));
  }

  private record Expiring(String value, long expires) {
  }

  private static @Nullable String take(Map<UUID, Expiring> map, UUID key, long now) {
    Expiring e = map.remove(key);
    return e == null || e.expires() <= now ? null : e.value();
  }

  @Override
  public CompletableFuture<Void> putRoute(UUID player, String server, long ttlMillis) {
    return run(() -> {
      routes.put(player, new Expiring(server, clock.getAsLong() + ttlMillis));
      return null;
    });
  }

  @Override
  public CompletableFuture<Boolean> useNonce(String nonce, long ttlMillis) {
    return run(() -> {
      long now = clock.getAsLong();
      Long until = nonces.get(nonce);
      if (until != null && until > now) {
        return false;
      }
      nonces.put(nonce, now + ttlMillis);
      return true;
    });
  }

  @Override
  public CompletableFuture<Void> putProfile(CachedProfile profile, long ttlMillis) {
    return run(() -> {
      profiles.put(profile.uuid(), profile);
      profileNames.put(profile.name().toLowerCase(Locale.ROOT), profile.uuid());
      return null;
    });
  }

  @Override
  public CompletableFuture<Optional<CachedProfile>> profile(UUID player) {
    return run(() -> Optional.ofNullable(profiles.get(player)));
  }

  @Override
  public CompletableFuture<Void> put(String key, String value, long ttlMillis) {
    return run(() -> {
      values.put(key, new Expiring(value, clock.getAsLong() + ttlMillis));
      return null;
    });
  }

  @Override
  public CompletableFuture<Optional<String>> get(String key) {
    return run(() -> {
      Expiring e = values.get(key);
      return e == null || e.expires() <= clock.getAsLong() ? Optional.<String>empty()
          : Optional.of(e.value());
    });
  }

  @Override
  public CompletableFuture<Optional<CachedProfile>> profileByName(String name) {
    return run(() -> Optional.ofNullable(profileNames.get(name.toLowerCase(Locale.ROOT)))
        .map(profiles::get));
  }

  @Override
  public CompletableFuture<Long> nextEpoch() {
    return run(() -> ++epoch);
  }

  @Override
  public CompletableFuture<Void> publish(String channel, byte[] payload) {
    return run(() -> List.copyOf(handlers.getOrDefault(channel, List.of())))
        .thenAccept(targets -> targets.forEach(h -> h.accept(payload)));
  }

  @Override
  public CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler) {
    synchronized (this) {
      handlers.computeIfAbsent(channel, k -> new CopyOnWriteArrayList<>()).add(handler);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void close() {
  }
}
