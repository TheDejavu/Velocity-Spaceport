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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.velocitypowered.proxy.VelocityServer;
import com.velocitypowered.proxy.config.VelocityConfiguration;
import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import com.velocitypowered.proxy.network.cluster.ClusterService;
import com.velocitypowered.proxy.network.cluster.VelocityNetwork;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Readiness, drain and the HTTP endpoints, on a mocked proxy.
 */
class ProxyHealthTest {

  private final VelocityServer server = mock(VelocityServer.class);
  private final ClusterService cluster = mock(ClusterService.class);
  private final HttpClient client = HttpClient.newHttpClient();
  private ProxyHealth health;

  @BeforeEach
  void start() throws Exception {
    VelocityConfiguration configuration = mock(VelocityConfiguration.class);
    when(configuration.getNetworkConfig()).thenReturn(new NetworkConfig(true, "p1", "", "sp:",
        "local", "newest-wins", "stop-logins", "127.0.0.1:0", 1, 30, "", 25565, 50,
        "network-secret.key", true, 21600, 300, false));
    when(server.getConfiguration()).thenReturn(configuration);
    when(server.getCluster()).thenReturn(cluster);
    VelocityNetwork network = mock(VelocityNetwork.class);
    when(network.getPlayerCount()).thenReturn(42);
    when(network.getProxies()).thenReturn(List.of());
    when(server.getNetwork()).thenReturn(network);
    when(server.getPlayerCount()).thenReturn(7);
    health = new ProxyHealth(server);
    health.start();
  }

  @AfterEach
  void stop() {
    health.close();
  }

  private HttpResponse<String> get(String path) throws Exception {
    return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"
        + health.address().getPort() + path)).build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void readyOnlyOnceStarted() throws Exception {
    assertEquals(200, get("/health/live").statusCode());
    HttpResponse<String> starting = get("/health/ready");
    assertEquals(503, starting.statusCode());
    assertTrue(starting.body().contains("starting"));
    health.markStarted();
    assertEquals(200, get("/health/ready").statusCode());
  }

  @Test
  void storeProblemMakesItNotReady() throws Exception {
    health.markStarted();
    when(cluster.readyProblem()).thenReturn("network store unreachable");
    HttpResponse<String> ready = get("/health/ready");
    assertEquals(503, ready.statusCode());
    assertTrue(ready.body().contains("network store unreachable"));
  }

  @Test
  void drainTurnsNotReadyAtOnceAndMarksDrainingAfterTheInterval() throws Exception {
    health.markStarted();
    final CompletableFuture<Void> marked = health.drain();
    assertEquals(503, get("/health/ready").statusCode());
    assertTrue(get("/health/ready").body().contains("draining"));
    verify(cluster, never()).setDraining(true);
    marked.get(3, TimeUnit.SECONDS);
    verify(cluster).setDraining(true);
    assertTrue(health.drain() == marked); // the same drain

    health.cancelDrain();
    verify(cluster, timeout(1000)).setDraining(false);
    assertFalse(health.isDraining());
    assertEquals(200, get("/health/ready").statusCode());
  }

  @Test
  void metrics() throws Exception {
    health.markStarted();
    health.loginCompleted();
    when(cluster.stats()).thenReturn(new ClusterService.Stats(3, 1, 0, 2, 150, 120, 9, 1, 0, 0, 0));
    String body = get("/metrics").body();
    assertTrue(body.contains("spaceport_ready 1\n"), body);
    assertTrue(body.contains("spaceport_players_local 7\n"), body);
    assertTrue(body.contains("spaceport_players_network 42\n"), body);
    assertTrue(body.contains("spaceport_logins_total 1\n"), body);
    assertTrue(body.contains("spaceport_claims_total{result=\"ok\"} 3\n"), body);
    assertTrue(body.contains("spaceport_store_latency_seconds 1.5E-4\n"), body);
    assertTrue(body.contains("spaceport_bus_timeouts_total 1\n"), body);
  }
}
