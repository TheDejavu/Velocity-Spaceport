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

import com.velocitypowered.proxy.config.VelocityConfiguration.NetworkConfig;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Owns this proxy's membership in the network: the store and the heartbeat.
 */
public final class ClusterService implements AutoCloseable {

  private static final Logger logger = LogManager.getLogger(ClusterService.class);
  static final long HEARTBEAT_MILLIS = 3_000;
  static final long HEARTBEAT_TTL_MILLIS = 15_000;

  private final String proxyId;
  private final NetworkStore store;
  private final long epoch;
  private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
    Thread t = new Thread(r, "Spaceport Network Heartbeat");
    t.setDaemon(true);
    return t;
  });

  /**
   * Connects to the network store and starts the heartbeat. Blocks; call at startup only.
   *
   * @param config the [network] section
   * @param bind the proxy's bind address, for the automatic id
   */
  public ClusterService(NetworkConfig config, InetSocketAddress bind) {
    this.proxyId = "auto".equals(config.proxyId()) ? autoId(bind) : config.proxyId();
    this.store = new RedisStore(config.redisUri(), config.keyPrefix());
    this.epoch = store.nextEpoch().join();
    store.heartbeat(proxyId, HEARTBEAT_TTL_MILLIS).join();
    timer.scheduleAtFixedRate(() -> store.heartbeat(proxyId, HEARTBEAT_TTL_MILLIS)
            .exceptionally(ex -> {
              logger.warn("Network heartbeat failed: {}", ex.toString());
              return null;
            }),
        HEARTBEAT_MILLIS, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS);
    logger.info("Joined the network as {} (epoch {})", proxyId, epoch);
  }

  private static String autoId(InetSocketAddress bind) {
    try {
      return InetAddress.getLocalHost().getHostName() + "-" + bind.getPort();
    } catch (UnknownHostException e) {
      return "proxy-" + bind.getPort();
    }
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

  @Override
  public void close() {
    timer.shutdownNow();
    store.close();
  }
}
