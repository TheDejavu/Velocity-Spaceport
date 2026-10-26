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

import static com.velocitypowered.proxy.VelocityServer.GENERAL_GSON;

import com.velocitypowered.api.event.network.NetworkPlayerJoinEvent;
import com.velocitypowered.api.event.network.NetworkPlayerQuitEvent;
import com.velocitypowered.api.event.network.NetworkPlayerServerChangeEvent;
import com.velocitypowered.api.network.LastSeen;
import com.velocitypowered.api.util.GameProfile;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import net.kyori.adventure.key.Key;
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

    /** Backend servers this proxy has players on. */
    Collection<String> localServers();

    /**
     * Players per backend server on this proxy.
     *
     * @return server name to player count, only servers with players
     */
    default Map<String, Integer> localServerCounts() {
      Map<String, Integer> counts = new HashMap<>();
      localServers().forEach(name -> counts.put(name, 1));
      return counts;
    }

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
  public static final Component RATE_LIMITED = Component.text(
      "Too many login attempts from your address, please wait a minute and try again.",
      NamedTextColor.RED);
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
  private final String instance = UUID.randomUUID().toString();
  private final MessageBus bus;
  private final NetworkConfig config;
  private final Map<UUID, TicketRoute> ticketRoutes = new ConcurrentHashMap<>();
  private volatile RouteTicket.@Nullable Keys ticketKeys;
  private final LongAdder ticketsAccepted = new LongAdder();
  private final LongAdder ticketsRejected = new LongAdder();
  private final LongAdder authSkipped = new LongAdder();
  private final LongAdder outageLogins = new LongAdder();
  private volatile boolean authOutage;
  private volatile boolean duplicateId;
  private volatile boolean draining;
  private final LongAdder claimsOk = new LongAdder();
  private final LongAdder claimsDenied = new LongAdder();
  private final LongAdder claimsUnavailable = new LongAdder();
  private final LongAdder storeErrors = new LongAdder();
  private final LongAdder loginsLimited = new LongAdder();
  private volatile long tickMicros;
  private volatile long tickMicrosAvg;
  private long lastClash;
  private long sweepPausedUntil;
  private volatile VelocityNetworkPlayer.@Nullable Actions actions;
  private volatile long epoch;
  private volatile @Nullable String generation;
  private volatile long lastTickOk;
  private volatile boolean storeDown;
  private volatile boolean pendingResync;
  private volatile boolean pendingResubscribe;
  private boolean tickInFlight;
  private volatile int remotePlayers;
  private volatile List<ProxyState> proxies = List.of();
  private final Map<String, BedrockDoors.Door> bedrockDoors = new ConcurrentHashMap<>();
  /** Players per backend server on each other proxy, from the last tick. */
  private volatile Map<String, Map<String, Integer>> remoteServerCounts = Map.of();

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
    this.bus = new MessageBus(store, proxyId, timer);
    this.config = config;
    this.authOutage = config.authOutageMode();
  }

  /**
   * Joins the network: drops what a previous run with the same id left behind, picks the epoch,
   * subscribes and starts ticking. Blocks for at most a few seconds; call at startup only. If the
   * store is unreachable the proxy still starts, not ready, and keeps trying.
   */
  public void start() {
    loadTicketKeys();
    refuseIfIdInUse();
    long wall = System.currentTimeMillis();
    try {
      // A previous run of this id may have crashed: its players are not here any more.
      int stale = store.sweep(proxyId, 0, true, config.lastServerTtl() * 1000L).get(5, TimeUnit.SECONDS);
      if (stale > 0) {
        logger.info("Removed {} players left in the network by a previous run of {}", stale,
            proxyId);
      }
      // Wall clock first, so a store that lost its counter cannot hand out an old epoch again.
      epoch = Math.max(wall, store.nextEpoch().get(5, TimeUnit.SECONDS));
      store.subscribe("msg:all", this::onMessage).get(5, TimeUnit.SECONDS);
      store.subscribe("msg:" + proxyId, this::onMessage).get(5, TimeUnit.SECONDS);
      Tick first = store.tick(proxyId, host.localPlayerCount(), encodedServers(), instance,
          draining, TTL_MILLIS, candidateGeneration).get(5, TimeUnit.SECONDS);
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
    long sent = System.nanoTime();
    store.tick(proxyId, host.localPlayerCount(), encodedServers(), instance, draining,
            TTL_MILLIS, candidateGeneration)
        .orTimeout(2, TimeUnit.SECONDS)
        .whenComplete((t, ex) -> timer.execute(() -> {
          tickInFlight = false;
          if (ex != null) {
            storeErrors.increment();
            if (!storeDown) {
              storeDown = true;
              logger.warn("Lost the network store: {}", rootMessage(ex));
            }
          } else {
            long micros = (System.nanoTime() - sent) / 1000;
            tickMicros = micros;
            tickMicrosAvg = tickMicrosAvg == 0 ? micros : (tickMicrosAvg * 15 + micros) / 16;
            applyTick(t);
          }
        }));
  }

  /** Servers as the tick carries them: "name=players". */
  private List<String> encodedServers() {
    List<String> out = new ArrayList<>();
    host.localServerCounts().forEach((name, n) -> out.add(name + "=" + n));
    return out;
  }

  private static int parseCount(String text) {
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return 0;
    }
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
    if (!t.previousInstance().isEmpty() && !t.previousInstance().equals(instance)) {
      lastClash = System.currentTimeMillis();
    }
    // Sticky: two writers alternate, so some ticks still see our own instance.
    boolean clash = lastClash != 0 && System.currentTimeMillis() - lastClash < DEAD_AFTER_MILLIS;
    if (clash != duplicateId) {
      duplicateId = clash;
      if (clash) {
        logger.error("ANOTHER LIVE PROXY IS USING THE NETWORK ID {}! This proxy stops taking"
            + " players until it is gone. Give every proxy its own [network] proxy-id.", proxyId);
      } else {
        logger.info("Network id {} is ours alone again", proxyId);
      }
    }
    if (storeDown) {
      storeDown = false;
      logger.info("Network store reachable again");
      // Every proxy was cut off just like us: give them time to tick before calling them dead.
      sweepPausedUntil = System.currentTimeMillis() + DEAD_AFTER_MILLIS + TICK_MILLIS * 4;
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
        if (now >= sweepPausedUntil && (last == null || now - last > DEAD_AFTER_MILLIS)) {
          sweepAttempts.put(p.id(), now);
          store.sweep(p.id(), DEAD_AFTER_MILLIS, false, config.lastServerTtl() * 1000L).thenAccept(n -> {
            if (n >= 0) {
              logger.info("Proxy {} is gone; released its {} players", p.id(), n);
            }
          });
        }
      }
    }
    remotePlayers = remote;
    List<ProxyState> decoded = new ArrayList<>(t.proxies().size());
    Map<String, Map<String, Integer>> counts = new HashMap<>();
    for (ProxyState p : t.proxies()) {
      List<String> names = new ArrayList<>(p.servers().size());
      Map<String, Integer> perServer = new HashMap<>();
      for (String entry : p.servers()) {
        int eq = entry.lastIndexOf('='); // the count is numeric, the name may hold '='
        String name = eq < 0 ? entry : entry.substring(0, eq);
        names.add(name);
        perServer.put(name, eq < 0 ? 1 : parseCount(entry.substring(eq + 1)));
      }
      decoded.add(new ProxyState(p.id(), p.players(), p.ageMillis(), List.copyOf(names),
          p.instance(), p.draining()));
      if (!p.id().equals(proxyId) && p.players() >= 0 && p.ageMillis() < DEAD_AFTER_MILLIS) {
        counts.put(p.id(), perServer);
      }
    }
    remoteServerCounts = counts;
    proxies = List.copyOf(decoded);
    publishBedrock();
    refreshBedrock();
    if (resync) {
      resync();
    }
  }

  /** Tells the other proxies where this proxy's Geyser is. */
  private void publishBedrock() {
    String host = config.bedrockHost();
    if (host == null || host.isEmpty()) {
      return;
    }
    store.put("bedrock:" + proxyId, host + ":" + config.bedrockPort(), TTL_MILLIS)
        .exceptionally(ex -> null);
  }

  /** Reads the Geysers the other proxies published. */
  private void refreshBedrock() {
    Set<String> live = new HashSet<>();
    for (ProxyState proxy : proxies) {
      if (proxy.id().equals(proxyId) || proxy.players() < 0 || proxy.ageMillis() >= DEAD_AFTER_MILLIS) {
        bedrockDoors.remove(proxy.id());
        continue;
      }
      live.add(proxy.id());
      store.get("bedrock:" + proxy.id()).thenAccept(value -> {
        BedrockDoors.Door door = BedrockDoors.parse(value.orElse(null));
        if (door == null) {
          bedrockDoors.remove(proxy.id());
        } else {
          bedrockDoors.put(proxy.id(), door);
        }
      });
    }
    bedrockDoors.keySet().retainAll(live);
  }

  /**
   * A Geyser that is up and not draining, on some other proxy.
   *
   * @return where a Bedrock player can be sent, or null
   */
  public BedrockDoors.@Nullable Door bedrockTarget() {
    List<BedrockDoors.Offer> offers = new ArrayList<>();
    for (ProxyState proxy : proxies) {
      boolean alive = proxy.players() >= 0 && proxy.ageMillis() < DEAD_AFTER_MILLIS;
      offers.add(new BedrockDoors.Offer(proxy.id(), alive, proxy.draining(),
          bedrockDoors.get(proxy.id())));
    }
    return BedrockDoors.pick(proxyId, offers);
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
   * @param properties the player's profile properties as JSON, for the shared profile cache
   * @return the session to pass to {@link #playerLeft} later, or the reason to deny the login
   */
  public CompletableFuture<LoginResult> claimLogin(UUID uuid, String name, String ip,
      @Nullable String properties) {
    PlayerRecord record = new PlayerRecord(uuid, name, proxyId,
        ThreadLocalRandom.current().nextLong(), null, ip, System.currentTimeMillis());
    CompletableFuture<LoginResult> result = new CompletableFuture<>();
    if (duplicateId || (!isReady() && !localOnlyWhenDown)) {
      claimsUnavailable.increment();
      result.complete(LoginResult.denied(UNAVAILABLE));
      return result;
    }
    new LoginClaim(record, properties, result).attempt();
    return result;
  }

  /**
   * The rate limit key of an address: IPv4 as is (also when IPv4-mapped), IPv6 by its /64 prefix,
   * without a scope id, so one customer cannot get a new allowance per address.
   *
   * @param ip a literal address
   * @return the key
   */
  static String rateKey(String ip) {
    int scope = ip.indexOf('%');
    String literal = scope < 0 ? ip : ip.substring(0, scope);
    if (literal.indexOf(':') < 0) {
      return literal;
    }
    try {
      byte[] b = java.net.InetAddress.getByName(literal).getAddress(); // a literal, no lookup
      if (b.length == 4) {
        return java.net.InetAddress.getByAddress(b).getHostAddress();
      }
      return String.format("%02x%02x:%02x%02x:%02x%02x:%02x%02x::/64", b[0], b[1], b[2], b[3],
          b[4], b[5], b[6], b[7]);
    } catch (java.net.UnknownHostException e) {
      return literal;
    }
  }

  /**
   * Shared login rate limit: counts a login attempt of an address across all proxies. Asks the
   * store without blocking, and fails open: if the limit is off, the store is down or does not
   * answer within a second, the login is allowed.
   *
   * @param ip the player's address
   * @return true if the login may go on, false if the address is over the limit
   */
  public CompletableFuture<Boolean> allowLogin(String ip) {
    if (!config.loginRateLimit() || storeDown) {
      return CompletableFuture.completedFuture(true);
    }
    return store.rateHit(rateKey(ip), config.loginRateWindow() * 1000L)
        .orTimeout(1, TimeUnit.SECONDS)
        .handle((hits, ex) -> {
          if (ex != null) {
            storeErrors.increment();
            return true;
          }
          if (hits > config.loginRateMax()) {
            loginsLimited.increment();
            return false;
          }
          return true;
        });
  }

  /**
   * Outcome of {@link #claimLogin}.
   *
   * @param session the session, if allowed
   * @param denyReason why the login is denied, or null
   * @param route the server the player should go to first (transfer or crashed proxy), or null
   */
  public record LoginResult(long session, @Nullable Component denyReason, @Nullable String route) {
    static LoginResult denied(Component reason) {
      return new LoginResult(0, reason, null);
    }
  }

  private final class LoginClaim {
    private final PlayerRecord record;
    private final @Nullable String properties;
    private final CompletableFuture<LoginResult> result;
    private final long start = System.currentTimeMillis();
    private long lastKick;
    private int failures;

    LoginClaim(PlayerRecord record, @Nullable String properties,
        CompletableFuture<LoginResult> result) {
      this.record = record;
      this.properties = properties;
      this.result = result;
    }

    void attempt() {
      store.claim(record, epoch, DEAD_AFTER_MILLIS, config.lastServerTtl() > 0)
          .whenComplete(this::claimed);
    }

    private void retry(long delayMillis) {
      timer.schedule(this::attempt, delayMillis, TimeUnit.MILLISECONDS);
    }

    private void claimed(@Nullable Claim c, @Nullable Throwable ex) {
      long now = System.currentTimeMillis();
      if (ex != null) {
        storeErrors.increment();
        if (now - start < LOGIN_BUDGET_MILLIS) {
          retry(Math.min(500, 50L << Math.min(failures++, 4)));
        } else if (localOnlyWhenDown) {
          logger.warn("Network store unreachable; letting {} in without a network check",
              record.name());
          pendingResync = true;
          admitted(null);
        } else {
          claimsUnavailable.increment();
          result.complete(LoginResult.denied(UNAVAILABLE));
        }
        return;
      }
      if (c.claimed()) {
        admitted(c.route());
        return;
      }
      if (!newestWins || now - start > KICK_WAIT_MILLIS) {
        claimsDenied.increment();
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

    private void admitted(@Nullable String storedRoute) {
      claimsOk.increment();
      Session s = new Session(record);
      sessions.put(record.uuid(), s);
      TicketRoute ticket = ticketRoutes.remove(record.uuid());
      String route = ticket != null && ticket.expires() > System.currentTimeMillis()
          ? ticket.server() : storedRoute;
      result.complete(new LoginResult(record.session(), null, route));
      if (properties != null) {
        store.putProfile(new NetworkStore.CachedProfile(record.uuid(), record.name(), properties),
            config.profileCacheTtl() * 1000L).exceptionally(ex -> null);
      }
      publish("msg:all", frame(JOIN, record.uuid(), record.name(), null, null, record.since()));
      host.fire(new NetworkPlayerJoinEvent(new VelocityNetworkPlayer(s.current(), actions)));
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
    host.fire(new NetworkPlayerServerChangeEvent(new VelocityNetworkPlayer(r, actions), previous));
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
    recordSeen(r);
    publish("msg:all", frame(QUIT, uuid, r.name(), r.server(), null, r.since()));
    host.fire(new NetworkPlayerQuitEvent(new VelocityNetworkPlayer(r, actions)));
  }

  /** Separates the fields of a last-seen record (a tab, which no name or server contains). */
  private static final String SEEN_SEP = String.valueOf((char) 9);

  /** Days the last-seen records live, 0 for none. */
  public int lastSeenDays() {
    return config.lastSeenTtl();
  }

  private void recordSeen(PlayerRecord r) {
    // A player that never reached a backend server was not really seen.
    if (config.lastSeenTtl() <= 0 || r.server() == null) {
      return;
    }
    long ttl = config.lastSeenTtl() * 86_400_000L;
    // Not atomic, but a quit of an older login only skips the write when it sees a newer record.
    store.get("seen:" + r.uuid()).thenCompose(old -> {
      if (old.map(v -> loginOf(v) > r.since()).orElse(false)) {
        return CompletableFuture.<Void>completedFuture(null);
      }
      return store.put("seen:" + r.uuid(), r.name() + SEEN_SEP + r.proxy() + SEEN_SEP
          + r.server() + SEEN_SEP + System.currentTimeMillis() + SEEN_SEP + r.since(), ttl)
          .thenCompose(v -> store.put("seenname:" + r.name().toLowerCase(Locale.ROOT),
              r.uuid().toString(), ttl));
    }).exceptionally(ex -> null);
  }

  /** The login time stored in a last-seen record, 0 if it has none. */
  private static long loginOf(String value) {
    String[] f = value.split(SEEN_SEP, -1);
    try {
      return f.length == 5 ? Long.parseLong(f[4]) : 0;
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Reads the record written when a player last left.
   *
   * @param uuid the player
   * @return the record (never marked online), or empty
   */
  public CompletableFuture<Optional<LastSeen>> lastSeen(UUID uuid) {
    return store.get("seen:" + uuid).thenApply(value -> value.flatMap(v -> {
      String[] f = v.split(SEEN_SEP, -1);
      if (f.length != 4 && f.length != 5) {
        return Optional.empty();
      }
      try {
        return Optional.of(new LastSeen(uuid, f[0], f[1], f[2].isEmpty() ? null : f[2],
            Instant.ofEpochMilli(Long.parseLong(f[3])), false));
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
    }));
  }

  /**
   * Finds a player that left within the retention time by name.
   *
   * @param name the player's name, ignoring case
   * @return its uuid, or empty
   */
  public CompletableFuture<Optional<UUID>> seenUuid(String name) {
    return store.get("seenname:" + name.toLowerCase(Locale.ROOT))
        .thenApply(v -> v.map(UUID::fromString));
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
      if (type == MessageBus.REQUEST || type == MessageBus.RESPONSE) {
        bus.onFrame(type, in);
        return;
      }
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
          0, server.isEmpty() ? null : server, "", since), actions);
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

  /**
   * Refuses to start when another proxy that is still running uses this proxy's id: its record
   * keeps being refreshed. A record left by a crashed previous run stops changing.
   */
  private void refuseIfIdInUse() {
    try {
      Optional<ProxyState> before = store.proxy(proxyId).get(5, TimeUnit.SECONDS);
      if (before.isEmpty() || before.get().ageMillis() >= DEAD_AFTER_MILLIS) {
        return;
      }
      Thread.sleep(4 * TICK_MILLIS);
      Optional<ProxyState> after = store.proxy(proxyId).get(5, TimeUnit.SECONDS);
      if (after.isPresent() && after.get().ageMillis() < 3 * TICK_MILLIS) {
        throw new IllegalStateException("Another live proxy is using the network id " + proxyId
            + ". Give every proxy its own [network] proxy-id.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
      logger.warn("Could not check whether the network id {} is free: {}", proxyId,
          rootMessage(e));
    }
  }

  public MessageBus bus() {
    return bus;
  }

  /** Name of the cookie that carries the transfer ticket. */
  public static final Key ROUTE_COOKIE = Key.key("spaceport", "route");

  /**
   * Whether Mojang outage mode is on (D9).
   *
   * @return true if on
   */
  public boolean authOutageMode() {
    return authOutage;
  }

  /**
   * Switches Mojang outage mode at runtime.
   *
   * @param on whether to let known players in while Mojang fails
   */
  public void setAuthOutageMode(boolean on) {
    authOutage = on;
    if (on) {
      logger.warn("MOJANG OUTAGE MODE ON: while Mojang's session servers fail, players whose last"
          + " good login was from the same address within {} s get in with their cached profile",
          config.authOutageWindow());
    } else {
      logger.info("Mojang outage mode off");
    }
  }

  /**
   * Remembers a login Mojang confirmed, for outage mode.
   *
   * @param uuid the player
   * @param name its name
   * @param ip its address
   */
  public void recordGoodLogin(UUID uuid, String name, String ip) {
    store.put("authok:" + name.toLowerCase(Locale.ROOT),
            uuid + " " + ip + " " + System.currentTimeMillis(), config.authOutageWindow() * 1000L)
        .exceptionally(ex -> null);
  }

  /**
   * In outage mode, the profile a player may log in with while Mojang fails: its last good login
   * must be from this address and recent enough. Every admission is logged.
   *
   * @param name the name the client logs in with
   * @param ip its address
   * @return the cached profile, or empty: then the login is refused as usual
   */
  public CompletableFuture<Optional<GameProfile>> outageLogin(String name, String ip) {
    if (!authOutage) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    return store.get("authok:" + name.toLowerCase(Locale.ROOT)).thenCompose(value -> {
      String[] parts = value.map(v -> v.split(" ")).orElse(new String[0]);
      long age = parts.length == 3 ? System.currentTimeMillis() - Long.parseLong(parts[2]) : -1;
      if (parts.length != 3 || !parts[1].equals(ip) || age < 0
          || age > config.authOutageWindow() * 1000L) {
        logger.warn("Mojang outage mode: {} ({}) has no recent good login from this address;"
            + " refused", name, ip);
        return CompletableFuture.completedFuture(Optional.<GameProfile>empty());
      }
      UUID uuid = UUID.fromString(parts[0]);
      return store.profile(uuid).thenApply(cached -> {
        outageLogins.increment();
        logger.warn("MOJANG OUTAGE MODE: letting {} ({}) in with its cached profile; last good"
            + " login from this address {} min ago", name, ip, age / 60_000);
        return Optional.of(new GameProfile(uuid, cached.map(NetworkStore.CachedProfile::name)
            .orElse(name), cached.map(c -> List.of(GENERAL_GSON.fromJson(c.properties(),
                GameProfile.Property[].class))).orElse(List.of())));
      });
    }).exceptionally(ex -> Optional.empty());
  }

  /**
   * Whether the last heartbeat reached the store.
   *
   * @return true if the store is reachable
   */
  public boolean storeReachable() {
    return !storeDown;
  }

  public boolean hasTicketKeys() {
    return ticketKeys != null;
  }

  /** Test hook: this proxy's clock for tickets is off by this much (-Dspaceport.network.test-clock-skew-ms). */
  private static final long TEST_CLOCK_SKEW_MILLIS =
      Long.getLong("spaceport.network.test-clock-skew-ms", 0);

  private static long ticketClock() {
    return System.currentTimeMillis() + TEST_CLOCK_SKEW_MILLIS;
  }

  /** Where a ticket said a player goes; kept until its login reaches the claim. */
  private record TicketRoute(String server, long expires) {
  }

  private void loadTicketKeys() {
    Path file = Path.of(config.transferSecretFile());
    try {
      boolean created = !Files.exists(file);
      ticketKeys = RouteTicket.Keys.load(file);
      if (created) {
        logger.warn("Created {} with a new transfer ticket key: copy it to every proxy of the"
            + " network", file);
      }
    } catch (IOException | RuntimeException e) {
      logger.error("Cannot use the transfer ticket keys in {}; transfers and ticket logins are"
          + " off (players log in normally)", file, e);
    }
  }

  /**
   * Whether drains transfer players (ticket keys loaded and a transfer address configured).
   *
   * @return true if players can be transferred
   */
  public boolean canTransfer() {
    return ticketKeys != null && !config.transferHost().isEmpty();
  }

  /**
   * Signs a ticket for a player about to be transferred, and leaves a route hint in the store for
   * a client that does not bring the cookie back.
   *
   * @param uuid the player
   * @param name its name
   * @param server its backend server, or null
   * @param ip its address
   * @return the cookie payload, or null if there are no keys
   */
  public byte @Nullable [] issueTicket(UUID uuid, String name, @Nullable String server,
      String ip) {
    RouteTicket.Keys keys = ticketKeys;
    if (keys == null) {
      return null;
    }
    if (server != null) {
      store.putRoute(uuid, server, 60_000).exceptionally(ex -> null);
    }
    return RouteTicket.issue(uuid, name, server == null ? "" : server, ip, proxyId,
        ticketClock()).sign(keys);
  }

  /**
   * A ticket that passed every check.
   *
   * @param ticket the ticket
   * @param properties the player's profile properties (JSON) from the shared cache, or null
   * @param nonceChecked whether the single-use nonce was checked; only then may the ticket
   *     replace the Mojang check (otherwise it only routes)
   */
  public record Redeemed(RouteTicket ticket, @Nullable String properties, boolean nonceChecked) {
  }

  /**
   * Checks a transfer ticket brought back by a client before encryption: signature, key, expiry,
   * the name and uuid the client logs in with, its address, and that its nonce was never used.
   * A good ticket's server becomes the player's first server.
   *
   * @param cookie the cookie payload, or null if the client had none
   * @param ip the client's address
   * @param name the name it logs in with
   * @param holder the uuid it logs in with, or null
   * @return the redeemed ticket, or empty: then the normal login applies
   */
  public CompletableFuture<Optional<Redeemed>> redeem(byte @Nullable [] cookie, String ip,
      String name, @Nullable UUID holder) {
    RouteTicket.Keys keys = ticketKeys;
    if (keys == null || cookie == null) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    long now = ticketClock();
    Optional<RouteTicket> verified = RouteTicket.verify(cookie, keys, now)
        .filter(t -> t.name().equalsIgnoreCase(name) && t.ip().equals(ip)
            && (holder == null || holder.equals(t.uuid())));
    if (verified.isEmpty()) {
      ticketsRejected.increment();
      return CompletableFuture.completedFuture(Optional.empty());
    }
    RouteTicket t = verified.get();
    return store.useNonce(t.nonce(), RouteTicket.LIFETIME_MILLIS + RouteTicket.SKEW_MILLIS)
        .handle((first, ex) -> ex != null ? null : first)
        .thenCompose(first -> {
          if (first == null) {
            // The store is unreachable (a failover): the ticket is genuine but its nonce cannot be
            // checked, so it routes the player but does not stand in for Mojang.
            if (!t.server().isEmpty()) {
              ticketRoutes.put(t.uuid(), new TicketRoute(t.server(), now + 60_000));
            }
            return CompletableFuture.completedFuture(Optional.of(new Redeemed(t, null, false)));
          }
          if (!first) {
            ticketsRejected.increment();
            return CompletableFuture.completedFuture(Optional.<Redeemed>empty());
          }
          return store.profile(t.uuid()).thenApply(p -> {
            ticketsAccepted.increment();
            if (!t.server().isEmpty()) {
              ticketRoutes.put(t.uuid(), new TicketRoute(t.server(), now + 60_000));
            }
            return Optional.of(new Redeemed(t, p.map(NetworkStore.CachedProfile::properties)
                .orElse(null), true));
          });
        })
        .exceptionally(ex -> {
          ticketsRejected.increment();
          return Optional.empty();
        });
  }

  /** A transferred player skipped the Mojang session check thanks to its ticket. */
  public void noteAuthSkipped() {
    authSkipped.increment();
  }

  /**
   * Gives up a player that is about to be transferred, so the next proxy can claim it at once.
   *
   * @param uuid the player
   * @return completes when released (or failed)
   */
  public CompletableFuture<Void> releaseForTransfer(UUID uuid) {
    Session s = sessions.get(uuid);
    if (s == null) {
      return CompletableFuture.completedFuture(null);
    }
    return store.release(uuid, proxyId, s.record.session()).handle((ok, ex) -> null);
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

  /**
   * Returns the number of players on a backend server on the other proxies, from the last tick.
   * Never waits on the store.
   *
   * @param server the backend server's name
   * @return the count on the other proxies
   */
  public int remotePlayerCount(String server) {
    int total = 0;
    for (Map<String, Integer> counts : remoteServerCounts.values()) {
      for (Map.Entry<String, Integer> e : counts.entrySet()) {
        if (e.getKey().equalsIgnoreCase(server)) {
          total += e.getValue();
        }
      }
    }
    return total;
  }

  /**
   * Returns the other proxies alive at the last tick.
   *
   * @return the live remote proxies
   */
  public List<ProxyState> liveRemotes() {
    return proxies.stream().filter(p -> !p.id().equals(proxyId) && p.players() >= 0
        && p.ageMillis() < DEAD_AFTER_MILLIS).toList();
  }

  /**
   * Sets what network players in events use for their actions.
   *
   * @param actions the actions
   */
  public void setActions(VelocityNetworkPlayer.Actions actions) {
    this.actions = actions;
  }

  public List<ProxyState> proxies() {
    return proxies;
  }

  /**
   * Why this proxy should not take new players, or null if it may. Being cut off from the store
   * counts only under the stop-logins policy, after {@link #READY_GRACE_MILLIS}.
   *
   * @return the reason, or null
   */
  public @Nullable String readyProblem() {
    if (duplicateId) {
      return "another live proxy uses this proxy-id";
    }
    if (!localOnlyWhenDown && storeDown
        && System.currentTimeMillis() - lastTickOk >= READY_GRACE_MILLIS) {
      return "network store unreachable";
    }
    return null;
  }

  /**
   * Marks this proxy draining in the network record (or not), from the next tick on.
   *
   * @param draining whether it is draining
   */
  public void setDraining(boolean draining) {
    this.draining = draining;
  }

  public boolean isDraining() {
    return draining;
  }

  /**
   * Counters for /metrics.
   *
   * @param claimsOk logins the network admitted
   * @param claimsDenied logins refused as already connected elsewhere
   * @param claimsUnavailable logins refused because the store was unreachable
   * @param storeErrors failed store calls (ticks and claims)
   * @param tickMicros last tick round trip
   * @param tickMicrosAvg smoothed tick round trip
   * @param busRequests requests sent to other proxies
   * @param busTimeouts requests that got no answer
   * @param ticketsAccepted transfer tickets that passed every check
   * @param ticketsRejected transfer tickets that were missing nothing but failed a check
   * @param authSkipped Mojang session checks skipped thanks to a ticket
   */
  public record Stats(long claimsOk, long claimsDenied, long claimsUnavailable, long storeErrors,
                      long tickMicros, long tickMicrosAvg, long busRequests, long busTimeouts,
                      long ticketsAccepted, long ticketsRejected, long authSkipped,
                      long outageLogins, long loginsLimited) {
  }

  /**
   * Returns the counters for /metrics.
   *
   * @return the counters
   */
  public Stats stats() {
    return new Stats(claimsOk.sum(), claimsDenied.sum(), claimsUnavailable.sum(), storeErrors.sum(),
        tickMicros, tickMicrosAvg, bus.requests(), bus.timeouts(), ticketsAccepted.sum(),
        ticketsRejected.sum(), authSkipped.sum(), outageLogins.sum(), loginsLimited.sum());
  }

  /**
   * Whether logins may be checked against the store: false once it has been unreachable for
   * {@link #READY_GRACE_MILLIS}, or while another proxy uses this id.
   *
   * @return true if ready
   */
  public boolean isReady() {
    return !duplicateId
        && (!storeDown || System.currentTimeMillis() - lastTickOk < READY_GRACE_MILLIS);
  }

  Optional<Session> session(UUID uuid) {
    return Optional.ofNullable(sessions.get(uuid));
  }

  /** Leaves the network: removes this proxy and its players from the store. */
  @Override
  public void close() {
    timer.shutdownNow();
    try {
      store.sweep(proxyId, 0, true, config.lastServerTtl() * 1000L).get(2, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.warn("Could not remove this proxy from the network store: {}", rootMessage(e));
    }
    store.close();
  }
}
