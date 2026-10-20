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
import io.lettuce.core.SetArgs;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * {@link NetworkStore} on Redis (standalone or Sentinel) through Lettuce. Lettuce runs on its own
 * small event loop group, never on the proxy's.
 */
public final class RedisStore implements NetworkStore {

  // Compare-and-set on the player hash: free, ours, or owned by the proxy we were told is dead.
  // Only KEYS[1] is touched, so it also runs on Cluster. Returns {claimed, previous/current owner}.
  private static final String CLAIM = """
      local owner = redis.call('HGET', KEYS[1], 'proxy')
      if owner and owner ~= ARGV[1] and owner ~= ARGV[3] then return {0, owner} end
      if owner == ARGV[1] and tonumber(redis.call('HGET', KEYS[1], 'epoch')) > tonumber(ARGV[2]) then
        return {0, owner}
      end
      redis.call('HSET', KEYS[1], 'proxy', ARGV[1], 'epoch', ARGV[2])
      return {1, owner or ''}
      """;

  private static final String RELEASE = """
      if redis.call('HGET', KEYS[1], 'proxy') == ARGV[1]
          and redis.call('HGET', KEYS[1], 'epoch') == ARGV[2] then
        return redis.call('DEL', KEYS[1])
      end
      return 0
      """;

  private static final RedisCodec<String, byte[]> CODEC =
      RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);

  private final String prefix;
  private final ClientResources resources;
  private final RedisClient client;
  private final StatefulRedisConnection<String, String> connection;
  private final RedisAsyncCommands<String, String> redis;
  private final StatefulRedisConnection<String, byte[]> publisher;
  private final StatefulRedisPubSubConnection<String, byte[]> subscriber;
  private final Map<String, Consumer<byte[]>> handlers = new ConcurrentHashMap<>();

  /**
   * Connects to Redis.
   *
   * @param uri a redis:// or redis-sentinel:// URI
   * @param prefix prefix for every key and channel
   */
  public RedisStore(String uri, String prefix) {
    this.prefix = prefix;
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
    // Fail fast while disconnected instead of queueing: logins stop rather than hang (D10).
    client.setOptions(ClientOptions.builder()
        .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
        .build());
    this.connection = client.connect();
    this.redis = connection.async();
    this.publisher = client.connect(CODEC);
    this.subscriber = client.connectPubSub(CODEC);
    subscriber.addListener(new RedisPubSubAdapter<>() {
      @Override
      public void message(String channel, byte[] message) {
        Consumer<byte[]> handler = handlers.get(channel);
        if (handler != null) {
          handler.accept(message);
        }
      }
    });
  }

  private String playerKey(UUID player) {
    return prefix + "player:{" + player + "}";
  }

  @Override
  public CompletableFuture<Void> heartbeat(String proxyId, long ttlMillis) {
    return redis.set(prefix + "proxy:" + proxyId, "1", SetArgs.Builder.px(ttlMillis))
        .toCompletableFuture().thenApply(ok -> null);
  }

  @Override
  public CompletableFuture<ClaimResult> claim(UUID player, String proxyId, long epoch) {
    return claim0(player, proxyId, epoch, "").thenCompose(result -> {
      if (result.claimed()) {
        return CompletableFuture.completedFuture(result);
      }
      String owner = result.owner();
      return redis.exists(prefix + "proxy:" + owner).toCompletableFuture().thenCompose(live ->
          live > 0 ? CompletableFuture.completedFuture(result)
              : claim0(player, proxyId, epoch, owner));
    });
  }

  private CompletableFuture<ClaimResult> claim0(UUID player, String proxyId, long epoch,
      String deadOwner) {
    // shortcut: EVAL sends the script body each time (~400 B); switch to EVALSHA with a NOSCRIPT
    // fallback if it ever shows up in profiles (scripts are not replicated to a new primary).
    return redis.<List<Object>>eval(CLAIM, ScriptOutputType.MULTI, new String[] {playerKey(player)},
            proxyId, Long.toString(epoch), deadOwner)
        .toCompletableFuture()
        .thenApply(r -> {
          String owner = (String) r.get(1);
          return new ClaimResult((Long) r.get(0) == 1, owner.isEmpty() ? null : owner);
        });
  }

  @Override
  public CompletableFuture<Boolean> release(UUID player, String proxyId, long epoch) {
    return redis.<Long>eval(RELEASE, ScriptOutputType.INTEGER, new String[] {playerKey(player)},
            proxyId, Long.toString(epoch))
        .toCompletableFuture().thenApply(n -> n > 0);
  }

  @Override
  public CompletableFuture<Long> nextEpoch() {
    return redis.incr(prefix + "epoch").toCompletableFuture();
  }

  @Override
  public CompletableFuture<Void> publish(String channel, byte[] payload) {
    return publisher.async().publish(prefix + channel, payload)
        .toCompletableFuture().thenApply(n -> null);
  }

  @Override
  public CompletableFuture<Void> subscribe(String channel, Consumer<byte[]> handler) {
    handlers.put(prefix + channel, handler);
    return subscriber.async().subscribe(prefix + channel).toCompletableFuture();
  }

  @Override
  public void close() {
    subscriber.close();
    publisher.close();
    connection.close();
    client.shutdown();
    resources.shutdown();
  }
}
