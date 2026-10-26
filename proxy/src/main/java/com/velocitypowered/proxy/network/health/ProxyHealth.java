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

package com.velocitypowered.proxy.network.health;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.velocitypowered.api.network.ProxyNode;
import com.velocitypowered.api.proxy.Player;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.network.cluster.ClusterService;
import com.velocitypowered.proxy.network.login.MojangNameLookup;
import com.velocitypowered.proxy.network.login.MojangNames;
import com.velocitypowered.proxy.util.AddressUtil;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Spaceport: readiness, drain mode and the health/metrics HTTP listener ({@code health-bind}).
 *
 * <p>Ready means: started, not draining, and (with the network on) not cut off from the store
 * under stop-logins and not clashing with another proxy's id. A drain turns not-ready at once,
 * waits {@code health-interval} so the load balancer stops sending players, and then marks this
 * proxy draining in the network. Players that still arrive are let in.
 */
public final class ProxyHealth implements AutoCloseable {

  private static final Logger logger = LogManager.getLogger(ProxyHealth.class);
  private static final int RATE_WINDOW = 10;

  private final VelocityServer server;
  private final LongAdder logins = new LongAdder();
  private final long[] loginSamples = new long[RATE_WINDOW + 1];
  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "Spaceport Health");
    t.setDaemon(true);
    return t;
  });
  private volatile boolean started;
  private volatile @Nullable CompletableFuture<Void> drain;
  private volatile @Nullable TransferDrain transfers;
  private volatile @Nullable CompletableFuture<Void> transfersDone;
  private final LongAdder transferred = new LongAdder();
  private final BedrockDetector bedrock;
  private final BedrockHandoff bedrockHandoff;
  private volatile double loginsPerSecond;
  private int sample;
  private volatile @Nullable HttpServer http;
  private volatile @Nullable ScheduledExecutorService probes;
  private final Map<String, Integer> backendUp = new ConcurrentHashMap<>();

  /**
   * Creates the health service; nothing runs until {@link #start()}.
   *
   * @param server the proxy
   */
  public ProxyHealth(VelocityServer server) {
    this.server = server;
    this.bedrock = new BedrockDetector(id -> server.getPluginManager().getPlugin(id)
        .flatMap(c -> c.getInstance()).map(instance -> instance.getClass().getClassLoader()));
    this.bedrockHandoff = new BedrockHandoff(id -> server.getPluginManager().getPlugin(id)
        .flatMap(c -> c.getInstance()).map(instance -> instance.getClass().getClassLoader()));
  }

  private NetworkConfig config() {
    return server.getConfiguration().getNetworkConfig();
  }

  /**
   * Opens the HTTP listener if {@code health-bind} is set.
   *
   * @throws IOException if the address cannot be bound
   */
  public void start() throws IOException {
    String bind = config().healthBind();
    if (bind.isEmpty()) {
      return;
    }
    timer.scheduleAtFixedRate(this::sampleLogins, 1, 1, TimeUnit.SECONDS);
    HttpServer http = HttpServer.create(AddressUtil.parseAddress(bind), 16);
    http.setExecutor(timer);
    http.createContext("/health/live", ex -> respond(ex, 200, "live\n"));
    http.createContext("/health/ready", ex -> {
      String problem = readyProblem();
      respond(ex, problem == null ? 200 : 503,
          problem == null ? "ready\n" : "not ready: " + problem + "\n");
    });
    http.createContext("/metrics", ex -> respond(ex, 200, metrics()));
    http.start();
    this.http = http;
    ScheduledExecutorService probe = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread thread = new Thread(r, "Spaceport Backend Probe");
      thread.setDaemon(true);
      return thread;
    });
    probe.scheduleWithFixedDelay(this::probeBackends, 0, 5, TimeUnit.SECONDS);
    this.probes = probe;
    logger.info("Health and metrics listening on http://{}/ (/health/live, /health/ready, "
        + "/metrics)", bind);
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  /**
   * Returns the address the HTTP listener is bound to.
   *
   * @return the address, or null if it is off
   */
  public @Nullable InetSocketAddress address() {
    HttpServer server = http;
    return server == null ? null : server.getAddress();
  }

  /** The proxy is listening for players. */
  public void markStarted() {
    started = true;
  }

  /** A player finished logging in to this proxy. */
  public void loginCompleted() {
    logins.increment();
  }

  private void sampleLogins() {
    long now = logins.sum();
    loginSamples[sample % loginSamples.length] = now;
    long old = loginSamples[(sample + 1) % loginSamples.length];
    sample++;
    loginsPerSecond = sample > RATE_WINDOW ? (now - old) / (double) RATE_WINDOW : 0;
  }

  /**
   * Returns why this proxy should not take new players, or null if it may.
   *
   * @return the reason, or null
   */
  public @Nullable String readyProblem() {
    if (!started) {
      return "starting";
    }
    if (drain != null) {
      return "draining";
    }
    ClusterService cluster = server.getCluster();
    return cluster == null ? null : cluster.readyProblem();
  }

  public boolean isDraining() {
    return drain != null;
  }

  /**
   * Starts draining: not ready at once, marked draining in the network after
   * {@code health-interval}. Calling it again returns the same drain.
   *
   * @return completes when the proxy is marked draining in the network
   */
  public synchronized CompletableFuture<Void> drain() {
    CompletableFuture<Void> current = drain;
    if (current != null) {
      return current;
    }
    CompletableFuture<Void> marked = new CompletableFuture<>();
    drain = marked;
    long deadline = System.currentTimeMillis() + config().drainTimeout() * 1000L;
    int wait = Math.min(config().healthInterval(), config().drainTimeout());
    logger.info("Draining: not ready any more; marking this proxy draining in {} s", wait);
    timer.schedule(() -> {
      if (drain != marked) {
        return; // cancelled meanwhile
      }
      ClusterService cluster = server.getCluster();
      if (cluster != null) {
        cluster.setDraining(true);
      }
      logger.info("Draining: marked draining in the network");
      if (cluster != null && (cluster.canTransfer() || !config().bedrockHost().isEmpty())) {
        transfers = new TransferDrain(server, cluster, timer, transferred, deadline, bedrock,
            bedrockHandoff);
        transfersDone = transfers.start();
      }
      marked.complete(null);
    }, wait, TimeUnit.SECONDS);
    return marked;
  }

  /**
   * A backend answered {@code spaceport:saved} for a player.
   *
   * @param player the player
   */
  public void saved(UUID player) {
    TransferDrain running = transfers;
    if (running != null) {
      running.saved(player);
    }
  }

  /**
   * Returns when the drain's transfers are over.
   *
   * @return completes when nobody is left or the drain timed out; done at once without a drain
   */
  public CompletableFuture<Void> drained() {
    CompletableFuture<Void> moving = transfersDone;
    return moving == null ? CompletableFuture.completedFuture(null) : moving;
  }

  /** Stops a drain: ready again (if nothing else is wrong) and no longer marked draining. */
  public synchronized void cancelDrain() {
    CompletableFuture<Void> current = drain;
    if (current == null) {
      return;
    }
    drain = null;
    current.cancel(false);
    TransferDrain running = transfers;
    if (running != null) {
      running.cancel();
      transfers = null;
    }
    ClusterService cluster = server.getCluster();
    if (cluster != null) {
      cluster.setDraining(false);
    }
    logger.info("Drain cancelled; taking players again");
  }

  /**
   * The drain step of a shutdown, before the listeners close: turns not-ready and waits until the
   * load balancer has noticed, at most {@code drain-timeout}. Does nothing when neither the
   * network nor the health listener is on (there is no balancer to tell).
   */
  public void drainForShutdown() {
    if (http == null && server.getCluster() == null) {
      return;
    }
    try {
      long deadline = System.currentTimeMillis() + Math.max(1, config().drainTimeout()) * 1000L;
      drain().get(Math.max(1, config().drainTimeout()), TimeUnit.SECONDS);
      // Phase 4: wait for the transfers (players that cannot be transferred are disconnected
      // with a reconnect message by the drain, the rest by the shutdown after the timeout).
      CompletableFuture<Void> moving = transfersDone;
      if (moving != null) {
        moving.get(Math.max(0, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      logger.warn("Drain before shutdown did not finish: {}", e.toString());
    }
  }

  /**
   * Returns the metrics in the Prometheus text format.
   *
   * @return the metrics
   */
  public String metrics() {
    StringBuilder out = new StringBuilder(1024);
    String problem = readyProblem();
    gauge(out, "spaceport_ready", "1 if this proxy takes new players", problem == null ? 1 : 0);
    gauge(out, "spaceport_draining", "1 while draining", isDraining() ? 1 : 0);
    gauge(out, "spaceport_players_local", "players on this proxy", server.getPlayerCount());
    gauge(out, "spaceport_players_network", "players on the whole network",
        server.getNetwork().getPlayerCount());
    gauge(out, "spaceport_proxies", "live proxies in the network, this one included",
        server.getNetwork().getProxies().size());
    perProxy(out, "spaceport_proxy_players", "players per proxy of the network", false);
    perProxy(out, "spaceport_proxy_draining", "1 while that proxy is draining", true);
    counter(out, "spaceport_logins_total", "logins completed on this proxy", logins.sum());
    counter(out, "spaceport_transfers_total", "players transferred away by drains",
        transferred.sum());
    gauge(out, "spaceport_logins_per_second", "logins per second over the last 10 s",
        loginsPerSecond);
    ClusterService cluster = server.getCluster();
    if (cluster != null) {
      ClusterService.Stats s = cluster.stats();
      out.append("# HELP spaceport_claims_total network login checks by result\n")
          .append("# TYPE spaceport_claims_total counter\n")
          .append("spaceport_claims_total{result=\"ok\"} ").append(s.claimsOk()).append('\n')
          .append("spaceport_claims_total{result=\"already_connected\"} ")
          .append(s.claimsDenied()).append('\n')
          .append("spaceport_claims_total{result=\"unavailable\"} ")
          .append(s.claimsUnavailable()).append('\n');
      gauge(out, "spaceport_store_up", "1 while the network store answers",
          cluster.storeReachable() ? 1 : 0);
      counter(out, "spaceport_store_errors_total", "failed network store calls", s.storeErrors());
      gauge(out, "spaceport_store_latency_seconds", "last heartbeat round trip",
          s.tickMicros() / 1e6);
      gauge(out, "spaceport_store_latency_avg_seconds", "smoothed heartbeat round trip",
          s.tickMicrosAvg() / 1e6);
      counter(out, "spaceport_bus_requests_total", "requests sent to other proxies",
          s.busRequests());
      counter(out, "spaceport_bus_timeouts_total", "requests to other proxies with no answer",
          s.busTimeouts());
      counter(out, "spaceport_tickets_accepted_total", "transfer tickets accepted",
          s.ticketsAccepted());
      counter(out, "spaceport_tickets_rejected_total", "transfer tickets that failed a check",
          s.ticketsRejected());
      counter(out, "spaceport_auth_skipped_total", "Mojang session checks skipped with a ticket",
          s.authSkipped());
      gauge(out, "spaceport_auth_outage_mode", "1 while Mojang outage mode is on",
          cluster.authOutageMode() ? 1 : 0);
      counter(out, "spaceport_auth_outage_logins_total", "logins let in by outage mode",
          s.outageLogins());
      counter(out, "spaceport_login_rate_limited_total",
          "logins refused by the shared per-address rate limit", s.loginsLimited());
    }
    mojangLookups(out);
    population(out);
    servers(out);
    return out.toString();
  }

  private void mojangLookups(StringBuilder out) {
    MojangNames names = server.getMojangNames();
    if (names == null) {
      return;
    }
    MojangNames.Stats s = names.stats();
    out.append("# HELP spaceport_mojang_lookup_total Mojang name lookups for offline logins\n")
        .append("# TYPE spaceport_mojang_lookup_total counter\n")
        .append("spaceport_mojang_lookup_total{result=\"cached\"} ").append(s.cacheHits())
        .append('\n')
        .append("spaceport_mojang_lookup_total{result=\"owned\"} ").append(s.owns())
        .append('\n')
        .append("spaceport_mojang_lookup_total{result=\"free\"} ").append(s.free())
        .append('\n')
        .append("spaceport_mojang_lookup_total{result=\"silent\"} ").append(s.silent())
        .append('\n');
    counter(out, "spaceport_mojang_rate_limited_total", "Mojang name lookups answered with 429",
        MojangNameLookup.rateLimited());
  }

  private void population(StringBuilder out) {
    int premium = 0;
    int cracked = 0;
    int bedrockPlayers = 0;
    Collection<Player> players = server.getAllPlayers();
    if (players != null) {
      for (Player player : players) {
        if (bedrock.isBedrock(player.getUniqueId())) {
          bedrockPlayers++;
        } else if (player.isOnlineMode()) {
          premium++;
        } else {
          cracked++;
        }
      }
    }
    out.append("# HELP spaceport_players_kind players on this proxy by how they joined\n")
        .append("# TYPE spaceport_players_kind gauge\n")
        .append("spaceport_players_kind{kind=\"premium\"} ").append(premium).append('\n')
        .append("spaceport_players_kind{kind=\"cracked\"} ").append(cracked).append('\n')
        .append("spaceport_players_kind{kind=\"bedrock\"} ").append(bedrockPlayers).append('\n');
  }

  private void servers(StringBuilder out) {
    out.append("# HELP spaceport_server_players players on this proxy, per backend\n")
        .append("# TYPE spaceport_server_players gauge\n")
        .append("# HELP spaceport_server_up 1 when this proxy can open a connection to the backend\n")
        .append("# TYPE spaceport_server_up gauge\n");
    Collection<RegisteredServer> servers = server.getAllServers();
    if (servers == null) {
      return;
    }
    for (RegisteredServer registered : servers) {
      String name = registered.getServerInfo().getName();
      if (name == null || name.isEmpty()) {
        continue;
      }
      Collection<Player> connected = registered.getPlayersConnected();
      int players = connected == null ? 0 : connected.size();
      String safe = label(name);
      out.append("spaceport_server_players{server=\"").append(safe).append("\"} ")
          .append(players).append('\n');
      Integer up = backendUp.get(name);
      if (up != null) {
        out.append("spaceport_server_up{server=\"").append(safe).append("\"} ")
            .append(up).append('\n');
      }
    }
  }

  private void probeBackends() {
    try {
      Collection<RegisteredServer> servers = server.getAllServers();
      if (servers == null) {
        return;
      }
      for (RegisteredServer registered : servers) {
        String name = registered.getServerInfo().getName();
        if (name == null || name.isEmpty()) {
          continue;
        }
        int up = 0;
        try (Socket socket = new Socket()) {
          socket.connect(registered.getServerInfo().getAddress(), 400);
          up = 1;
        } catch (IOException failure) {
          up = 0;
        }
        backendUp.put(name, up);
      }
    } catch (RuntimeException failure) {
      logger.warn("Backend probe failed: {}", failure.toString());
    }
  }

  private void perProxy(StringBuilder out, String name, String help, boolean draining) {
    out.append("# HELP ").append(name).append(' ').append(help).append('\n')
        .append("# TYPE ").append(name).append(" gauge").append('\n');
    for (ProxyNode node : server.getNetwork().getProxies()) {
      out.append(name).append("{proxy=").append("\"").append(label(node.getId()))
          .append("\"").append("} ")
          .append(draining ? (node.isDraining() ? 1 : 0) : node.getPlayerCount()).append('\n');
    }
  }

  private static String label(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  private static void gauge(StringBuilder out, String name, String help, double value) {
    line(out, name, help, "gauge", value);
  }

  private static void counter(StringBuilder out, String name, String help, double value) {
    line(out, name, help, "counter", value);
  }

  private static void line(StringBuilder out, String name, String help, String type,
      double value) {
    out.append("# HELP ").append(name).append(' ').append(help).append('\n')
        .append("# TYPE ").append(name).append(' ').append(type).append('\n')
        .append(name).append(' ');
    if (value == Math.rint(value)) {
      out.append((long) value);
    } else {
      out.append(value);
    }
    out.append('\n');
  }

  @Override
  public void close() {
    if (http != null) {
      http.stop(0);
    }
    timer.shutdownNow();
    ScheduledExecutorService probe = probes;
    if (probe != null) {
      probe.shutdownNow();
    }
  }
}
