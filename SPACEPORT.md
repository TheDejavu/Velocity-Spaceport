# Spaceport

A [Velocity](https://github.com/PaperMC/Velocity) fork (`dev/4.0.0`) focused on lower CPU and
memory use and better stability. Every change is a small commit on top of upstream, and every
behaviour change can be turned off with a system property.

## JVM flag

```
--sun-misc-unsafe-memory-access=allow
```

Silences the JDK warning when Netty uses `sun.misc.Unsafe` (see `spaceport.netty-unsafe`).

## Changes

| Property (default) | What it does |
|---|---|
| `spaceport.netty-unsafe` (`true`) | Re-enables Netty's Unsafe on Java 25+, where Netty turns it off and every buffer copy allocates NIO views. Ignored if `io.netty.noUnsafe` is set. |
| `spaceport.write-coalescing` (`true`) | Packets written between two flushes go into one buffer: one socket write and one cipher call per flush instead of per packet. |
| `spaceport.serverbound-flush-batching` (`true`) | Client-to-backend packets flush once per read instead of once per packet. |
| fix | Compressed frames claiming more than DEFLATE can produce (over 1032x) are rejected before allocating. Off with `velocity.skip-uncompressed-packet-size-validation`. |
| fix | Chat or commands arriving while the player has no backend (kicked, mid-switch) are dropped instead of disconnecting the player with "An internal error occurred". |

## Results

500 bots through the proxy, two Paper backends, Spaceport and stock side by side:

| | Stock | Spaceport |
|---|---|---|
| CPU per packet | baseline | -6 % |
| Allocation per packet | 213 B | 56 B (-74 %) |
| GC per 90 s | 11 / 140 ms | 5 / 55 ms |
| RSS | baseline | -250 to -300 MB |

Online mode (encryption) and real terrain were not measured yet.

## Upstream pull requests included early

Dropped once upstream merges them.

| PR | Fixes |
|---|---|
| [#1908](https://github.com/PaperMC/Velocity/pull/1908) | 254/382-byte handshakes (e.g. long Floodgate hostnames) misread as legacy pings. |
| [#1885](https://github.com/PaperMC/Velocity/pull/1885) | Log injection through control characters in usernames. |
| [#1782](https://github.com/PaperMC/Velocity/pull/1782) | `ChatQueue` blocking the event loop; rate-limited commands arriving out of order. |
