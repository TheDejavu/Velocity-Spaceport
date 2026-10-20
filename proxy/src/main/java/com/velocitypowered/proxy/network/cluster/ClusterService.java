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

import com.velocitypowered.api.event.network.NetworkPlayerJoinEvent;
import com.velocitypowered.api.event.network.NetworkPlayerQuitEvent;
import com.velocitypowered.api.event.network.NetworkPlayerServerChangeEvent;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.network.cluster.NetworkStore.Claim;
import com.velocitypowered.proxy.network.cluster.NetworkStore.PlayerRecord;
import com.velocitypowered.proxy.network.cluster.NetworkStore.ProxyState;
import com.velocitypowered.proxy.network.cluster.NetworkStore.Tick;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This proxy's membership in the network: heartbeat (tick), presence of its players, dead-proxy
 * sweep, resync after the store lost data, the cached network count and the network events.
 *
 * <p>Threading: store callbacks run on the store's threads, the tick on {@link #timer}. Nothing
 * here runs on, or blocks, a proxy event loop; callers hop back themselves.
 */
public final class ClusterService implements AutoCloseable {

  /** What the service needs from the proxy. */
  public interface Host {
    int localPlayerCount();

    /** Disconnects a local player that logged in elsewhere. */
    void disconnectDuplicate(UUID player);

    void fire(Object event);
  }

  private static final Logger logger = LogManager.getLogger(ClusterService.class);
  static final long TICK_MILLIS = 250;
  static final long TTL_MILLIS = 15_000;
  /** A proxy silent this long is dead: its players can log in elsewhere and it gets swept. */
  static final long DEAD_AFTER_MILLIS = 5_000;
  /** How long a login keeps retrying while the store is unreachable (a failover takes 3-7 s). */
  static final long LOGIN_BUDGET_MILLIS = 5_000;
  /** How long a login waits for the old session to go (newest wins); > dead-after. */
  static final long KICK_WAIT_MILLIS = 7_000;
  /** Not ready (stop new logins, fail health checks) once the store is unreachable this long. */
  static final long READY_GRACE_MILLIS = 10_000;

  private static final byte JOIN = 1;
  private static final byte QUIT = 2;
  private static final byte SWITCH = 3;
  private static final byte KICK = 4;

  public static final Component UNAVAILABLE = Component.text(
      "The network is unavailable right now, please try again in a moment.", NamedTextColor.RED);
  static final Component ALREADY_CONNECTED =
      Component.translatable("velocity.error.already-connected-proxy", NamedTextColor.RED);

  private final String proxyId;
  private final NetworkStore store;
  private final Host host;
  private final boolean newestWins;
  private final boolean localOnlyWhenDown;
  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "Spaceport Network");
    t.setDaemon(true);
    return t;
  });
  private final Map<UUID, Session> sessions = new ConcurrentHashMap<>();
  private final Map<String, Long> sweepAttempts = new HashMap<>();
  private final String candidateGeneration = UUID.randomUUID().toString();
  private volatile long epoch;
  private volatile @Nullable String generation;
  private volatile long lastTickOk;
  private volatile boolean storeDown;
  private volatile boolean pendingResync;
  private volatile boolean pendingResubscribe;
  private boolean tickInFlight;
  private volatile int remotePlayers;
  private volatile List<ProxyState> proxies = List.of();

  /** A login of a local player, as written to the store. */
  static final class Session {
    final PlayerRecord record;
    volatile @Nullable String server;

    Session(PlayerRecord record) {
      this.record = record;
    }

    PlayerRecord current() {
      PlayerRecord r = record;
      return new PlayerRecord(r.uuid(), r.name(), r.proxy(), r.session(), server, r.ip(),
          r.since());
    }
  }

  /**
   * Creates the service. Call {@link #start()} to join the network.
   *
   * @param config the [network] section
   * @param proxyId this proxy's id
   * @param store the network store
   * @param host the proxy
   */
  public ClusterService(NetworkConfig config, String proxyId, NetworkStore store, Host host) {
    this.proxyId = proxyId;
    this.store = store;
    this.host = host;
    this.newestWins = config.newestWins();
    this.localOnlyWhenDown = config.localOnlyWhenDown();
  }

  /**
   * Joins the network: drops what a previous run with the same id left behind, picks the epoch,
   * subscribes and starts ticking. Blocks for at most a few seconds; call at startup only. If the
   * store is unreachable the proxy still starts, not ready, and keeps trying.
   */
  public void start() {
    long wall = System.currentTimeMillis();
    try {
      // A previous run of this id may have crashed: its players are not here any more.
      int stale = store.sweep(proxyId, 0, true).get(5, TimeUnit.SECONDS);
      if (stale > 0) {
        logger.info("Removed {} players left in the network by a previous run of {}", stale,
            proxyId);
      }
      // Wall clock first, so a store that lost its counter cannot hand out an old epoch again.
      epoch = Math.max(wall, store.nextEpoch().get(5, TimeUnit.SECONDS));
      store.subscribe("msg:all", this::onMessage).get(5, TimeUnit.SECONDS);
      store.subscribe("msg:" + proxyId, this::onMessage).get(5, TimeUnit.SECONDS);
      Tick first = store.tick(proxyId, host.localPlayerCount(), TTL_MILLIS, candidateGeneration)
          .get(5, TimeUnit.SECONDS);
      if (first.selfExisted()) {
        logger.error("Another live proxy uses the network id {}; give every proxy its own"
            + " proxy-id", proxyId);
      }
      applyTick(first);
      logger.info("Joined the network as {} ({} proxies, {} players)", proxyId,
          first.proxies().size(), getPlayerCount());
    } catch (Exception e) {
      epoch = Math.max(epoch, wall);
      storeDown = true;
      logger.error("Could not reach the network store; this proxy is not ready until it can",
          e);
      // Subscriptions are retried by the tick loop below.
      pendingResubscribe = true;
    }
    timer.scheduleWithFixedDelay(this::tick, TICK_MILLIS, TICK_MILLIS, TimeUnit.MILLISECONDS);
  }

  private void tick() {
    if (pendingResubscribe) {
      pendingResubscribe = false;
      for (String channel : List.of("msg:all", "msg:" + proxyId)) {
        store.subscribe(channel, this::onMessage).exceptionally(ex -> {
          pendingResubscribe = true;
          return null;
        });
      }
    }
    if (tickInFlight) {
      return;
    }
    tickInFlight = true;
    store.tick(proxyId, host.localPlayerCount(), TTL_MILLIS, candidateGeneration)
        .orTimeout(2, TimeUnit.SECONDS)
        .whenComplete((t, ex) -> timer.execute(() -> {
          tickInFlight = false;
          if (ex != null) {
            if (!storeDown) {
              storeDown = true;
              logger.warn("Lost the network store: {}", rootMessage(ex));
            }
          } else {
            applyTick(t);
          }
        }));
  }

  private static String rootMessage(Throwable e) {
    Throwable c = e;
    while (c.getCause() != null) {
      c = c.getCause();
    }
    return c.toString();
  }

  void applyTick(Tick t) {
    String known = generation;
    boolean resync = false;
    if (known != null && !known.equals(t.generation())) {
      logger.warn("The network store lost its data (generation changed); re-asserting {} players",
          sessions.size());
      resync = true;
    } else if (known != null && !t.selfExisted()) {
      logger.warn("This proxy's network record was gone (expired or swept); re-asserting {}"
          + " players", sessions.size());
      resync = true;
    }
    generation = t.generation();
    if (storeDown) {
      storeDown = false;
      logger.info("Network store reachable again");
    }
    lastTickOk = System.currentTimeMillis();
    if (pendingResync) {
      resync = true;
    }

    int remote = 0;
    long now = System.currentTimeMillis();
    for (ProxyState p : t.proxies()) {
      if (p.id().equals(proxyId)) {
        continue;
      }
      if (p.players() >= 0 && p.ageMillis() < DEAD_AFTER_MILLIS) {
        remote += p.players();
      } else {
        Long last = sweepAttempts.get(p.id());
        if (last == null || now - last > DEAD_AFTER_MILLIS) {
          sweepAttempts.put(p.id(), now);
          store.sweep(p.id(), DEAD_AFTER_MILLIS, false).thenAccept(n -> {
            if (n >= 0) {
              logger.info("Proxy {} is gone; released its {} players", p.id(), n);
            }
          });
        }
      }
    }
    remotePlayers = remote;
    proxies = List.copyOf(t.proxies());
    if (resync) {
      resync();
    }
  }

  /** Writes every local player again, after the store lost them. */
  void resync() {
    pendingResync = false;
    for (Session s : sessions.values()) {
      store.claim(s.current(), epoch, DEAD_AFTER_MILLIS).whenComplete((c, ex) -> {
        if (ex != null) {
          pendingResync = true;
        } else if (!c.claimed() && sessions.get(s.record.uuid()) == s) {
          // Logged in on another proxy while the store had lost this one: only one may stay.
          logger.warn("{} is now on proxy {} too; disconnecting it here", s.record.name(),
              c.owner());
          host.disconnectDuplicate(s.record.uuid());
        }
      });
    }
  }

  /**
   * Network check for a login, after authentication and before the player is registered. On
   * success the player is owned by this proxy with the returned session.
   *
   * @param uuid the player
   * @param name the player's name
   * @param ip the player's address
   * @return the session to pass to {@link #playerLeft} later, or the reason to deny the login
   */
  public CompletableFuture<LoginResult> claimLogin(UUID uuid, String name, String ip) {
    PlayerRecord record = new PlayerRecord(uuid, name, proxyId,
        ThreadLocalRandom.current().nextLong(), null, ip, System.currentTimeMillis());
    CompletableFuture<LoginResult> result = new CompletableFuture<>();
    if (!isReady() && !localOnlyWhenDown) {
      result.complete(LoginResult.denied(UNAVAILABLE));
      return result;
    }
    new LoginClaim(record, result).attempt();
    return result;
  }

  /**
   * Outcome of {@link #claimLogin}.
   *
   * @param session the session, if allowed
   * @param denyReason why the login is denied, or null
   */
  public record LoginResult(long session, @Nullable Component denyReason) {
    static LoginResult denied(Component reason) {
      return new LoginResult(0, reason);
    }
  }

  private final class LoginClaim {
    private final PlayerRecord record;
    private final CompletableFuture<LoginResult> result;
    private final long start = System.currentTimeMillis();
    private long lastKick;
    private int failures;

    LoginClaim(PlayerRecord record, CompletableFuture<LoginResult> result) {
      this.record = record;
      this.result = result;
    }

    void attempt() {
      store.claim(record, epoch, DEAD_AFTER_MILLIS).whenComplete(this::claimed);
    }

    private void retry(long delayMillis) {
      timer.schedule(this::attempt, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void claimed(@Nullable Claim c, @Nullable Throwable ex) {
      long now = System.currentTimeMillis();
      if (ex != null) {
        if (now - start < LOGIN_BUDGET_MILLIS) {
          retry(Math.min(500, 50L << Math.min(failures++, 4)));
        } else if (localOnlyWhenDown) {
          logger.warn("Network store unreachable; letting {} in without a network check",
              record.name());
          pendingResync = true;
          admitted();
        } else {
          result.complete(LoginResult.denied(UNAVAILABLE));
        }
        return;
      }
      if (c.claimed()) {
        admitted();
        return;
      }
      if (!newestWins || now - start > KICK_WAIT_MILLIS) {
        result.complete(LoginResult.denied(ALREADY_CONNECTED));
        return;
      }
      // Newest wins: ask the owner to let go, then claim again until it has (or it died).
      if (now - lastKick >= 1000) {
        lastKick = now;
        publish("msg:" + c.owner(), frame(KICK, record.uuid(), null, null, null, 0));
      }
      retry(100);
    }

    private void admitted() {
      Session s = new Session(record);
      sessions.put(record.uuid(), s);
      result.complete(new LoginResult(record.session(), null));
      publish("msg:all", frame(JOIN, record.uuid(), record.name(), null, null, record.since()));
      host.fire(new NetworkPlayerJoinEvent(new VelocityNetworkPlayer(s.current())));
    }
  }

  /**
   * A local player connected to a backend server.
   *
   * @param uuid the player
   * @param session its session
   * @param server the new server
   */
  public void serverChanged(UUID uuid, long session, String server) {
    Session s = sessions.get(uuid);
    if (s == null || s.record.session() != session) {
      return;
    }
    String previous = s.server;
    s.server = server;
    store.updateServer(uuid, proxyId, session, server).exceptionally(ex -> {
      pendingResync = true; // the resync writes the server too
      return null;
    });
    PlayerRecord r = s.current();
    publish("msg:all", frame(SWITCH, uuid, r.name(), server, previous, r.since()));
    host.fire(new NetworkPlayerServerChangeEvent(new VelocityNetworkPlayer(r), previous));
  }

  /**
   * A local player left (or its login failed after {@link #claimLogin} allowed it).
   *
   * @param uuid the player
   * @param session its session
   */
  public void playerLeft(UUID uuid, long session) {
    Session s = sessions.get(uuid);
    if (s == null || s.record.session() != session || !sessions.remove(uuid, s)) {
      return;
    }
    release(uuid, session, 0);
    PlayerRecord r = s.current();
    publish("msg:all", frame(QUIT, uuid, r.name(), r.server(), null, r.since()));
    host.fire(new NetworkPlayerQuitEvent(new VelocityNetworkPlayer(r)));
  }

  private void release(UUID uuid, long session, int attempt) {
    store.release(uuid, proxyId, session).exceptionally(ex -> {
      // A record left behind would block this player elsewhere until the owner answers a kick
      // (see onMessage), so keep trying for a while.
      if (attempt < 20) {
        timer.schedule(() -> release(uuid, session, attempt + 1), 500, TimeUnit.MILLISECONDS);
      }
      return false;
    });
  }

  private void publish(String channel, byte[] frame) {
    store.publish(channel, frame).exceptionally(ex -> null); // best effort, never the only copy
  }

  private byte[] frame(byte type, UUID uuid, @Nullable String name, @Nullable String server,
      @Nullable String previous, long since) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(96);
      DataOutputStream out = new DataOutputStream(bytes);
      out.writeByte(type);
      out.writeUTF(proxyId);
      out.writeLong(uuid.getMostSignificantBits());
      out.writeLong(uuid.getLeastSignificantBits());
      out.writeUTF(name == null ? "" : name);
      out.writeUTF(server == null ? "" : server);
      out.writeUTF(previous == null ? "" : previous);
      out.writeLong(since);
      return bytes.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  void onMessage(byte[] payload) {
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
      byte type = in.readByte();
      String from = in.readUTF();
      UUID uuid = new UUID(in.readLong(), in.readLong());
      String name = in.readUTF();
      String server = in.readUTF();
      String previous = in.readUTF();
      long since = in.readLong();
      if (from.equals(proxyId)) {
        return;
      }
      VelocityNetworkPlayer player = new VelocityNetworkPlayer(new PlayerRecord(uuid, name, from,
          0, server.isEmpty() ? null : server, "", since));
      switch (type) {
        case JOIN -> host.fire(new NetworkPlayerJoinEvent(player));
        case QUIT -> host.fire(new NetworkPlayerQuitEvent(player));
        case SWITCH -> host.fire(new NetworkPlayerServerChangeEvent(player,
            previous.isEmpty() ? null : previous));
        case KICK -> kickRequested(uuid);
        default -> {
          // newer proxy version, ignore
        }
      }
    } catch (IOException | RuntimeException e) {
      logger.warn("Bad network message", e);
    }
  }

  private void kickRequested(UUID uuid) {
    if (sessions.containsKey(uuid)) {
      host.disconnectDuplicate(uuid); // its teardown releases the record
      return;
    }
    // Not here: drop a record a failed release may have left under our name.
    store.player(uuid).thenAccept(r -> r.filter(p -> p.proxy().equals(proxyId)
            && !sessions.containsKey(uuid))
        .ifPresent(p -> store.release(uuid, proxyId, p.session())));
  }

  public String proxyId() {
    return proxyId;
  }

  public long epoch() {
    return epoch;
  }

  public NetworkStore store() {
    return store;
  }

  /**
   * Returns the network player count from the last tick plus this proxy's live count. Never
   * waits on the store.
   *
   * @return the count
   */
  public int getPlayerCount() {
    return remotePlayers + host.localPlayerCount();
  }

  public List<ProxyState> proxies() {
    return proxies;
  }

  /**
   * Whether this proxy should take new players: false once the store has been unreachable for
   * {@link #READY_GRACE_MILLIS}.
   *
   * @return true if ready
   */
  public boolean isReady() {
    return !storeDown || System.currentTimeMillis() - lastTickOk < READY_GRACE_MILLIS;
  }

  Optional<Session> session(UUID uuid) {
    return Optional.ofNullable(sessions.get(uuid));
  }

  /** Leaves the network: removes this proxy and its players from the store. */
  @Override
  public void close() {
    timer.shutdownNow();
    try {
      store.sweep(proxyId, 0, true).get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.warn("Could not remove this proxy from the network store: {}", rootMessage(e));
    }
    store.close();
  }
}
