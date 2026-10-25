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

### Plugin API

`proxyServer.getNetwork()` covers what plugins use RedisBungee for, without a separate plugin. It works with the network off too: it then answers for this proxy alone and the futures complete normally. Counts are cached and never wait on Redis; lookups return a `CompletableFuture`.

| RedisBungee feature | Spaceport API |
| --- | --- |
| Player count of the network | `getPlayerCount()` |
| Player count of a server or proxy | `getPlayerCount(server)`, `getPlayerCountOnProxy(proxyId)` |
| Player list of the network, a server, a proxy | `getPlayers()`, `getPlayers(server)`, `getPlayersOnProxy(proxyId)` |
| Proxy of a player, server of a player | `getPlayer(uuid or name)` gives a `NetworkPlayer` with `getProxyId()`, `getServerName()` |
| Is a player online | `isOnline(uuid or name)` |
| Last online, last server | `getLastSeen(uuid or name)`, `getLastServer(uuid)` |
| Name to UUID and back | `getUniqueId(name)`, `getUsername(uuid)` |
| Proxy ids and their counts | `getProxies()`, `getProxy(id)` |
| Plugin messaging between proxies | `getMessenger()`: `registerListener`, `send`, `broadcast`, `request` |
| Join, leave and server switch events | `NetworkPlayerJoinEvent`, `NetworkPlayerQuitEvent`, `NetworkPlayerServerChangeEvent` (also for players on other proxies) |

Last-seen records are written when a player leaves and kept for `last-seen-ttl` days (default 30, `0` turns them off). Name lookups check the players online, then the shared profile cache, then the last-seen records, so they also find players who left a while ago.

Messages use a channel such as `myplugin:chat`. `send` goes to one proxy, `broadcast` to all other proxies, `request` waits for the answer of a handler on one proxy. Payloads are at most 32 KiB. Messages are acknowledged and retried for a few seconds, never run twice, and have no order; nothing is kept for a proxy that is down. Listeners run on their own threads, never on a network thread. Call `unregisterAll(plugin)` when your plugin stops.

```java
NetworkMessenger messenger = server.getNetwork().getMessenger();
Key channel = Key.key("myplugin", "ping");

// on every proxy: answer pings, and log announcements
messenger.registerRequestHandler(this, channel, (ch, from, payload) ->
    ("pong from " + server.getNetwork().getProxyId()).getBytes(StandardCharsets.UTF_8));
messenger.registerListener(this, Key.key("myplugin", "announce"), (ch, from, payload) ->
    logger.info("{} says {}", from, new String(payload, StandardCharsets.UTF_8)));

// tell all other proxies, then ask one of them
messenger.broadcast(Key.key("myplugin", "announce"), "hello".getBytes(StandardCharsets.UTF_8));
messenger.request(channel, "proxy-2", new byte[0], Duration.ofSeconds(3))
    .thenAccept(answer -> logger.info(new String(answer, StandardCharsets.UTF_8)))
    .exceptionally(e -> { logger.warn("no answer: {}", e.toString()); return null; });

server.getNetwork().getLastSeen("Steve").thenAccept(last -> last.ifPresent(l ->
    logger.info("{} was on {} / {} at {}", l.getUsername(), l.getProxyId(),
        l.getServerName().orElse("-"), l.getTime())));
```

Network events have one limit: a proxy that crashes does not send leave events for its players. Their records are removed after the other proxies notice (a few seconds), and they can log in elsewhere at once.

### Shared login rate limit

Off by default. With `login-rate-limit = true` one address may start at most `login-rate-max` logins (default 20) per `login-rate-window` seconds (default 60), counted across all proxies, so a bot cannot get a fresh allowance by hitting each proxy in turn. It is checked right after the login packet, before the Mojang check, and does not replace Velocity's own `login-ratelimit`. Extra logins are refused with a "too many login attempts" message. The counter is in Redis and asked without blocking; if Redis is unreachable or slow the limit is skipped and logins go through. Refusals show up as `spaceport_login_rate_limited_total`. Do not set it too low for players who share an address.

### Metrics and dashboard

`/metrics` also has `spaceport_proxy_players{proxy}` and `spaceport_proxy_draining{proxy}` for every proxy, `spaceport_store_up`, and the existing store latency, login, claim and ticket counters. `docs/spaceport-grafana.json` is a ready Grafana 10+ dashboard for them (import it and pick your Prometheus data source).

### Restarts without refused connections

With `enable-reuse-port = true` (Linux, native transport; ignored on NIO and Windows with a log note) a new proxy process can bind the same port while the old one is still draining. Start the new process, then `/spaceport drain` the old one and stop it once it is empty. Off by default.

## Upstream pull requests included early

Dropped once upstream merges them.

| PR | Fixes |
|---|---|
| [#1908](https://github.com/PaperMC/Velocity/pull/1908) | 254/382-byte handshakes (e.g. long Floodgate hostnames) misread as legacy pings. |
| [#1885](https://github.com/PaperMC/Velocity/pull/1885) | Log injection through control characters in usernames. |
| [#1891](https://github.com/PaperMC/Velocity/pull/1891) | Client settings sent during configuration not reaching the backend (raw translation keys). |
| [#1782](https://github.com/PaperMC/Velocity/pull/1782) | `ChatQueue` blocking the event loop; rate-limited commands arriving out of order. |
