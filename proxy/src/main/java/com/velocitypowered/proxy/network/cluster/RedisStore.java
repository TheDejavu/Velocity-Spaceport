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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link NetworkStore} on Redis (standalone or Sentinel) through Lettuce. Lettuce runs on its own
 * small event loop group, never on the proxy's.
 *
 * <p>Keys: {@code <prefix>{net}:proxies} (set of ids), {@code {net}:proxy:<id>} (hash: players,
 * seen; TTL), {@code {net}:members:<id>} (uuids the proxy owns), {@code {net}:gen},
 * {@code {net}:epoch}, {@code player:{<uuid>}} (hash: proxy, epoch, session, name, server, ip,
 * since) and {@code name:<lowercase name>} (uuid). Scripts touch keys across hash slots, so this
 * targets standalone and Sentinel, not Cluster.
 */
public final class RedisStore implements NetworkStore {

  // All scripts below are fixed text run with EVAL; arguments are passed as data.
  private static final String NOW = "local t = redis.call('TIME') "
      + "local now = t[1] * 1000 + math.floor(t[2] / 1000)\n";

  // KEYS: proxies set, gen, own proxy hash. ARGV: id, players, ttl, candidate gen, proxy prefix,
  // servers (comma separated), instance, draining (1 or 0).
  private static final String TICK = NOW + """
      local created = 0
      local g = redis.call('GET', KEYS[2])
      if not g then redis.call('SET', KEYS[2], ARGV[4]); g = ARGV[4]; created = 1 end
      local existed = redis.call('EXISTS', KEYS[3])
      local previous = redis.call('HGET', KEYS[3], 'instance') or ''
      redis.call('HSET', KEYS[3], 'players', ARGV[2], 'seen', now, 'servers', ARGV[6],
          'instance', ARGV[7], 'draining', ARGV[8])
      redis.call('PEXPIRE', KEYS[3], ARGV[3])
      redis.call('SADD', KEYS[1], ARGV[1])
      local out = {g, created, existed, previous}
      for _, id in ipairs(redis.call('SMEMBERS', KEYS[1])) do
        local v = redis.call('HMGET', ARGV[5] .. id, 'players', 'seen', 'servers', 'instance',
            'draining')
        out[#out + 1] = id
        out[#out + 1] = v[1] or '-1'
        out[#out + 1] = v[2] and tostring(now - tonumber(v[2])) or '-1'
        out[#out + 1] = v[3] or ''
        out[#out + 1] = v[4] or ''
        out[#out + 1] = v[5] or '0'
      end
      return out
      """;

  // KEYS: proxy hash. Returns {players, age, servers, instance} or an empty list.
  private static final String PROXY = NOW + """
      local v = redis.call('HMGET', KEYS[1], 'players', 'seen', 'servers', 'instance', 'draining')
      if not v[2] then return {} end
      return {v[1], tostring(now - tonumber(v[2])), v[3] or '', v[4] or '', v[5] or '0'}
      """;

  // KEYS: player hash, name key, own members set. ARGV: proxy, epoch, session, name, server, ip,
  // since, deadAfter, proxy prefix, members prefix, uuid.
  private static final String CLAIM = NOW + """
      local cur = redis.call('HMGET', KEYS[1], 'proxy', 'epoch')
      local owner = cur[1]
      if owner and owner ~= ARGV[1] then
        local seen = redis.call('HGET', ARGV[9] .. owner, 'seen')
        if seen and now - tonumber(seen) < tonumber(ARGV[8]) then return {0, owner} end
        redis.call('SREM', ARGV[10] .. owner, ARGV[11])
      end
      if owner == ARGV[1] and tonumber(cur[2]) > tonumber(ARGV[2]) then return {0, owner} end
      redis.call('DEL', KEYS[1])
      redis.call('HSET', KEYS[1], 'proxy', ARGV[1], 'epoch', ARGV[2], 'session', ARGV[3],
          'name', ARGV[4], 'server', ARGV[5], 'ip', ARGV[6], 'since', ARGV[7])
      redis.call('SET', KEYS[2], ARGV[11])
      redis.call('SADD', KEYS[3], ARGV[11])
      return {1, owner or ''}
      """;

  // KEYS: player hash. ARGV: proxy, session, server.
  private static final String UPDATE = """
      local cur = redis.call('HMGET', KEYS[1], 'proxy', 'session')
      if cur[1] ~= ARGV[1] or cur[2] ~= ARGV[2] then return 0 end
      redis.call('HSET', KEYS[1], 'server', ARGV[3])
      return 1
      """;

  // KEYS: player hash, name key, own members set. ARGV: proxy, session, uuid.
  private static final String RELEASE = """
      local cur = redis.call('HMGET', KEYS[1], 'proxy', 'session')
      if cur[1] ~= ARGV[1] or cur[2] ~= ARGV[2] then return 0 end
      redis.call('DEL', KEYS[1])
      if redis.call('GET', KEYS[2]) == ARGV[3] then redis.call('DEL', KEYS[2]) end
      redis.call('SREM', KEYS[3], ARGV[3])
      return 1
      """;

  // KEYS: members set, proxies set, proxy hash. ARGV: id, deadAfter, force, player prefix,
  // name prefix.
  private static final String SWEEP = NOW + """
      local seen = redis.call('HGET', KEYS[3], 'seen')
      if ARGV[3] ~= '1' and seen and now - tonumber(seen) < tonumber(ARGV[2]) then return -1 end
      local n = 0
      for _, uuid in ipairs(redis.call('SMEMBERS', KEYS[1])) do
        local key = ARGV[4] .. '{' .. uuid .. '}'
        local cur = redis.call('HMGET', key, 'proxy', 'name')
        if cur[1] == ARGV[1] then
          redis.call('DEL', key)
          if cur[2] then
            local nk = ARGV[5] .. string.lower(cur[2])
            if redis.call('GET', nk) == uuid then redis.call('DEL', nk) end
          end
          n = n + 1
        end
      end
      redis.call('DEL', KEYS[1], KEYS[3])
      redis.call('SREM', KEYS[2], ARGV[1])
      return n
      """;

  private static final RedisCodec<String, byte[]> CODEC =
      RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

  private static final Logger logger = LogManager.getLogger(RedisStore.class);

  private final String prefix;
  private final String net;
  private final ClientResources resources;
  private final RedisClient client;
  private final Map<String, Consumer<byte[]>> handlers = new ConcurrentHashMap<>();
  private volatile @Nullable Connections connections;
  private volatile boolean closed;

  private record Connections(StatefulRedisConnection<String, String> commands,
                             StatefulRedisConnection<String, byte[]> publisher,
                             StatefulRedisPubSubConnection<String, byte[]> subscriber) {
  }

  /**
   * Connects to Redis. If Redis is unreachable the store starts anyway and keeps trying in the
   * background; until then every call fails.
   *
   * @param uri a redis:// or redis-sentinel:// URI
   * @param prefix prefix for every key and channel
   */
  public RedisStore(String uri, String prefix) {
    this.prefix = prefix;
    this.net = prefix + "{net}:";
    this.resources = DefaultClientResources.builder()
        .ioThreadPoolSize(2)
        .computationThreadPoolSize(2)
        // The default backoff grows to 30 s; after a Sentinel failover that left us 3-4 s behind
        // Jedis. Retry at least every 250 ms.
        .reconnectDelay(Delay.exponential(Duration.ofMillis(50), Duration.ofMillis(250), 2,
            TimeUnit.MILLISECONDS))
        .build();
    RedisURI redisUri = RedisURI.create(uri);
    redisUri.setTimeout(Duration.ofSeconds(2));
    this.client = RedisClient.create(resources, redisUri);
    // Fail fast while disconnected instead of queueing; callers retry within their own budget.
    client.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .build());
    if (!connect()) {
      // Lettuce only reconnects connections it once had; keep trying the first one ourselves.
      Thread.ofPlatform().daemon().name("Spaceport Redis Connect").start(() -> {
        while (!closed && !connect()) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return;
          }
        }
      });
    }
  }

  private boolean connect() {
    try {
      StatefulRedisPubSubConnection<String, byte[]> subscriber = client.connectPubSub(CODEC);
      subscriber.addListener(new RedisPubSubAdapter<>() {
        @Override
        public void message(String channel, byte[] message) {
          Consumer<byte[]> handler = handlers.get(channel);
          if (handler != null) {
            handler.accept(message);
          }
        }
      });
      Connections c = new Connections(client.connect(), client.connect(CODEC), subscriber);
      if (!handlers.isEmpty()) {
        subscriber.sync().subscribe(handlers.keySet().toArray(String[]::new));
      }
      connections = c;
      return true;
    } catch (RuntimeException e) {
      logger.warn("Cannot connect to Redis yet: {}", e.getMessage());
      return false;
    }
  }

  private <T> CompletableFuture<T> call(
      Function<RedisAsyncCommands<String, String>, CompletionStage<T>> op) {
    Connections c = connections;
    if (c == null) {
      return CompletableFuture.failedFuture(new IllegalStateException("not connected to Redis"));
    }
    return op.apply(c.commands().async()).toCompletableFuture();
  }

  private String playerKey(UUID player) {
    return prefix + "player:{" + player + "}";
  }

  private String nameKey(String name) {
    return prefix + "name:" + name.toLowerCase(Locale.ROOT);
  }

  @Override
  public CompletableFuture<Tick> tick(String proxyId, int players, Collection<String> servers,
      String instance, boolean draining, long ttlMillis, String candidateGeneration) {
    return call(redis -> redis.<List<Object>>eval(TICK, ScriptOutputType.MULTI,
            new String[] {net + "proxies", net + "gen", net + "proxy:" + proxyId},
            proxyId, Integer.toString(players), Long.toString(ttlMillis), candidateGeneration,
            net + "proxy:", String.join(",", servers), instance, draining ? "1" : "0"))
        .thenApply(r -> {
          List<ProxyState> proxies = new ArrayList<>();
          for (int i = 4; i + 5 < r.size(); i += 6) {
            proxies.add(new ProxyState((String) r.get(i), Integer.parseInt((String) r.get(i + 1)),
                Long.parseLong((String) r.get(i + 2)), servers((String) r.get(i + 3)),
                (String) r.get(i + 4), "1".equals(r.get(i + 5))));
          }
          return new Tick((String) r.get(0), (Long) r.get(1) == 1, (Long) r.get(2) == 1,
              (String) r.get(3), proxies);
        });
  }

  private static List<String> servers(String joined) {
    return joined.isEmpty() ? List.of() : List.of(joined.split(","));
  }

  @Override
  public CompletableFuture<Optional<ProxyState>> proxy(String proxyId) {
    return call(redis -> redis.<List<Object>>eval(PROXY, ScriptOutputType.MULTI,
            new String[] {net + "proxy:" + proxyId}))
        .thenApply(r -> r.isEmpty() ? Optional.empty() : Optional.of(new ProxyState(proxyId,
            Integer.parseInt((String) r.get(0)), Long.parseLong((String) r.get(1)),
            servers((String) r.get(2)), (String) r.get(3), "1".equals(r.get(4)))));
  }

  @Override
  public CompletableFuture<Claim> claim(PlayerRecord p, long epoch, long deadAfterMillis) {
    return call(redis -> redis.<List<Object>>eval(CLAIM, ScriptOutputType.MULTI,
            new String[] {playerKey(p.uuid()), nameKey(p.name()), net + "members:" + p.proxy()},
            p.proxy(), Long.toString(epoch), Long.toString(p.session()), p.name(),
            p.server() == null ? "" : p.server(), p.ip(), Long.toString(p.since()),
            Long.toString(deadAfterMillis), net + "proxy:", net + "members:", p.uuid().toString()))
        .thenApply(r -> {
          String owner = (String) r.get(1);
          return new Claim((Long) r.get(0) == 1, owner.isEmpty() ? null : owner);
        });
  }

  @Override
  public CompletableFuture<Boolean> updateServer(UUID player, String proxyId, long session,
      String server) {
    return call(redis -> redis.<Long>eval(UPDATE, ScriptOutputType.INTEGER,
            new String[] {playerKey(player)}, proxyId, Long.toString(session), server))
        .thenApply(n -> n == 1);
  }

  @Override
  public CompletableFuture<Boolean> release(UUID player, String proxyId, long session) {
    return call(redis -> redis.hget(playerKey(player), "name"))
        .thenCompose(name -> call(redis -> redis.<Long>eval(RELEASE, ScriptOutputType.INTEGER,
            new String[] {playerKey(player), name == null ? prefix + "name:" : nameKey(name),
                net + "members:" + proxyId},
            proxyId, Long.toString(session), player.toString())))
        .thenApply(n -> n == 1);
  }

  @Override
  public CompletableFuture<Integer> sweep(String proxyId, long deadAfterMillis, boolean force) {
    return call(redis -> redis.<Long>eval(SWEEP, ScriptOutputType.INTEGER,
            new String[] {net + "members:" + proxyId, net + "proxies", net + "proxy:" + proxyId},
            proxyId, Long.toString(deadAfterMillis), force ? "1" : "0", prefix + "player:",
            prefix + "name:"))
        .thenApply(Long::intValue);
  }

  @Override
  public CompletableFuture<Optional<PlayerRecord>> player(UUID player) {
    return call(redis -> redis.hgetall(playerKey(player))).thenApply(h -> {
      if (h == null || h.get("proxy") == null) {
        return Optional.empty();
      }
      String server = h.get("server");
      return Optional.of(new PlayerRecord(player, h.get("name"), h.get("proxy"),
          Long.parseLong(h.get("session")), server == null || server.isEmpty() ? null : server,
          h.get("ip"), Long.parseLong(h.get("since"))));
    });
  }

  @Override
  public CompletableFuture<Optional<UUID>> uuidOf(String name) {
    return call(redis -> redis.get(nameKey(name)))
        .thenApply(s -> Optional.ofNullable(s).map(UUID::fromString));
  }

  @Override
  public CompletableFuture<Long> nextEpoch() {
    return call(redis -> redis.incr(net + "epoch"));
  }

  @Override
  public CompletableFuture<Void> publish(String channel, byte[] payload) {
    Connections c = connections;
    if (c == null) {
      return CompletableFuture.failedFuture(new IllegalStateException("not connected to Redis"));
    }
    return c.publisher().async().publish(prefix + channel, payload)
        .toCompletableFuture().thenApply(n -> null);
  }

  /**
   * Subscribes to a channel. Before the first connection the subscription is only recorded and
   * made when Redis is reached; Lettuce renews subscriptions after reconnects itself.
   */
  @Override
  public CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler) {
    handlers.put(prefix + channel, handler);
    Connections c = connections;
    if (c == null) {
      return CompletableFuture.completedFuture(null);
    }
    return c.subscriber().async().subscribe(prefix + channel).toCompletableFuture();
  }

  @Override
  public void close() {
    closed = true;
    Connections c = connections;
    if (c != null) {
      c.subscriber().close();
      c.publisher().close();
      c.commands().close();
    }
    client.shutdown();
    resources.shutdown();
  }
}
