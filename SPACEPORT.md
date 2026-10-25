# Spaceport

A [Velocity](https://github.com/PaperMC/Velocity) fork (`dev/4.0.0`) focused on lower CPU and
memory use and better stability. Every change is a small commit on top of upstream, and every
behaviour change can be turned off with a system property.

## JVM flag

```
--sun-misc-unsafe-memory-access=allow
```

Silences the JDK warning when Netty uses `sun.misc.Unsafe` (see `spaceport.netty-unsafe`). Start
with `java -jar`; otherwise also add `--add-opens java.base/java.nio=ALL-UNNAMED`.

## Changes

| Property (default) | What it does |
|---|---|
| `spaceport.netty-unsafe` (`true`) | Re-enables Netty's Unsafe on Java 25+, where Netty turns it off and every buffer copy allocates NIO views. Direct buffers stay outside `MaxDirectMemorySize` as before (no `System.gc()` storms under load). Ignored if `io.netty.noUnsafe` is set. |
| `spaceport.write-coalescing` (`true`) | Packets written between two flushes go into one buffer: one socket write and one cipher call per flush instead of per packet. |
| `spaceport.passthrough-fast-path` (`true`) | Packets Velocity does not handle go straight from the backend to the player's encoders, skipping the decoder and handler chain. Only used while neither connection has handlers added by plugins (ViaVersion, packetevents, ...). |
| `spaceport.compressed-passthrough` (`true`) | With the fast path and equal compression thresholds on both sides, compressed packets are forwarded as they are instead of being inflated and deflated again. |
| `spaceport.share-tab-profiles` (`true`) | Players listed in many tab lists share one copy of their profile and skin data instead of one per viewer. With 2,500 players on busy servers: heap in use 2.6 GB -> 0.4 GB, GC pauses -93 %. |
| `spaceport.raw-player-info` (`true`) | Player info (tab list) packets are forwarded as received instead of re-encoded with every skin; Velocity still reads them to track tab lists. -12 % CPU per packet on busy tab lists. |
| `spaceport.offline-encryption` (`false`) | Encrypts offline-mode connections from 1.20.5+ clients (no Mojang authentication). |
| `spaceport.backend-read-timeout` (`20000`) | Once a player is playing on a backend, that connection times out after at most this many milliseconds, before the client's own 30 s timeout. Joining keeps `read-timeout`. `0` uses `read-timeout` throughout. |
| `spaceport.failover-on-backend-timeout` (`true`) | A backend that stops responding sends its players to the next server in the try list while their clients are still connected, instead of disconnecting them. |
| `spaceport.unwritable-timeout` (`30000`) | A client that has not read its pending data for this many milliseconds is disconnected ("Timed out") instead of holding its buffers. `0` turns it off. |
| `spaceport.client-write-buffer` (`0`) | Per-player write buffer in bytes before backend reads pause; `0` keeps Velocity's 2 MiB. Lower uses less memory under bursts but costs some CPU. |
| `spaceport.aesni-cipher` (`true`) | Encrypted connections use an AES-NI implementation of AES/CFB8 on x86-64 (OpenSSL 3 builds) instead of OpenSSL's generic one: about 1.7x faster to encrypt, 7x to decrypt. |
| `spaceport.serverbound-flush-batching` (`true`) | Client-to-backend packets flush once per read instead of once per packet. |
| fix | Compressed frames claiming more than DEFLATE can produce (over 1032x) are rejected before allocating. Off with `velocity.skip-uncompressed-packet-size-validation`. |
| perf | The configuration-phase tags packet is forwarded without being parsed and re-encoded. |
| fix | Chat or commands arriving while the player has no backend (kicked, mid-switch) are dropped instead of disconnecting the player with "An internal error occurred". |

## Results

Two proxies side by side against the same Paper backends with a real terrain world, 300 bots
each:

| | Stock | Spaceport |
|---|---|---|
| CPU per packet | baseline | -15 % |
| CPU per packet, encrypted connections | baseline | -18 % |
| Allocation per packet | 216 B | 75 B |
| GC per 150 s | 10 / 125 ms | 5 / 55 ms |

One proxy on 16 threads, lightweight test backends sending about 450 packets per second to each
player:

| Players | Stock | Spaceport |
|---|---|---|
| 1,000 | 8.7 cores | 1.5 cores |
| 2,500 | saturated at about 2,400 players (16 cores, commands take 0.5 s) | 4.3 cores |
| 5,000 | saturated, logins time out | 9.5 cores, 2.3 M packets/s |

## Network (multi-proxy)

Run 2-5 proxies behind a load balancer as one network: one shared player list (counts, `/find`, `/send`, `/glist`, plugin messages), one login per player across all proxies, and restarts without losing players. A proxy that drains signs a short-lived ticket for each player and transfers them to the balancer address (1.20.5+ clients, no second Mojang check); older clients get a reconnect message. A crashed proxy's players are sent back to their last server when they reconnect.

Off by default (`enabled = false`); a single proxy behaves as before. Needs Redis, and for failover **Redis Sentinel** (a plain Redis works but is a single point of failure). Enable it on every proxy, with its own `proxy-id`:

```toml
accepts-transfers = true            # top level, on every proxy

[network]
enabled = true
proxy-id = "proxy-1"
redis-uri = "redis-sentinel://password@host1:26379,host2:26379,host3:26379/0#mymaster"
health-bind = "127.0.0.1:25890"
transfer-host = "play.example.com"  # the load balancer address
transfer-port = 25565
transfer-secret-file = "network-secret.key"   # same file on every proxy
```

All keys are documented in `velocity.toml`. Point the balancer's health check at `/health/ready`; `/health/live` and `/metrics` (Prometheus) are on the same `health-bind` listener.

To restart a proxy, run `/spaceport drain` on its console: it turns not-ready, transfers its players to the balancer, and the proxy can then be stopped (a normal shutdown drains too). `/spaceport drain cancel` aborts, `/spaceport network` shows the proxies and players per proxy.

## Upstream pull requests included early

Dropped once upstream merges them.

| PR | Fixes |
|---|---|
| [#1908](https://github.com/PaperMC/Velocity/pull/1908) | 254/382-byte handshakes (e.g. long Floodgate hostnames) misread as legacy pings. |
| [#1885](https://github.com/PaperMC/Velocity/pull/1885) | Log injection through control characters in usernames. |
| [#1891](https://github.com/PaperMC/Velocity/pull/1891) | Client settings sent during configuration not reaching the backend (raw translation keys). |
| [#1782](https://github.com/PaperMC/Velocity/pull/1782) | `ChatQueue` blocking the event loop; rate-limited commands arriving out of order. |
