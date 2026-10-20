# FeedGuard

**Know when your market data is lying to you.**

FeedGuard is a probe that watches crypto exchange order-book feeds and flags the failures that
latency dashboards miss: dropped updates, corrupted books, silent stalls and API format changes.

A slow feed costs you a few milliseconds. A feed that silently skips an update leaves your local
order book wrong until you notice — and your strategy keeps trading on it.

- Java 21, **zero runtime dependencies** (JDK WebSocket + a small built-in JSON parser)
- Binance spot and OKX today
- Automatic book resync after every incident, with time-to-recover measured

> **Status: early prototype.** Detection logic is covered by unit and fault-injection tests;
> long-running live validation is in progress. Expect rough edges.

## What it detects

| Incident | Binance spot (`@depth@100ms`) | OKX (`books`) |
|---|---|---|
| `SEQUENCE_GAP` | first update id `U` ≠ previous `u` + 1 on a live book | `prevSeqId` ≠ previous `seqId` |
| `CHECKSUM_MISMATCH` | — (Binance publishes no checksum) | CRC32 of top 25 levels ≠ venue checksum |
| `CHECKSUM_UNAVAILABLE` | — | venue sends `checksum: 0` ([deprecated by OKX](https://www.okx.com/en-gb/help/okx-order-book-channels-checksum-field-deprecation) since 23 June 2026); integrity relies on `seqId` only (reported once) |
| `BOOK_MISMATCH` | live book ≠ reference rebuilt from a fresh REST snapshot + the same events, at an equal update id | live book ≠ reference from a short-lived second subscription, at an equal `seqId` |
| `CROSSED_BOOK` / `BOOK_UNCROSSED` | best bid ≥ best ask on a synced book | same |
| `STALE_FEED` / `FEED_RECOVERED` | no message for longer than the threshold, even with the socket open | same |
| `SCHEMA_DRIFT` | fields added or removed vs. the first message seen | same, per action |
| `DISCONNECT` / `RECONNECTED` | socket closed or failed; exponential-backoff reconnect | same |
| `BOOK_SYNCED` | book (re)built, with resync time after an incident | same |
| `SNAPSHOT_FAILED`, `VENUE_ERROR`, `PARSE_ERROR` | REST errors, venue error payloads, unreadable messages | same |

### Why reconciliation

Sequence ids prove that nothing was lost on the wire. They do not prove that the book you hold is
right: an update applied twice, a level not removed, a bug in your book code. A venue checksum
used to catch that; OKX deprecated its checksum in June 2026 and Binance never had one.

Every `--reconcile-sec` seconds FeedGuard builds an independent reference book and compares the
top 20 levels with the live book **at exactly the same venue sequence**, so a moving market never
causes a false alarm. A mismatch raises `BOOK_MISMATCH` and rebuilds the live book. Rounds that
cannot align within 15 s count as inconclusive, never as incidents. Totals are printed as
`[recon] ... verified=N mismatch=N inconclusive=N`.

Design notes:

- **A stale snapshot is not a feed gap.** At startup Binance's REST snapshot can predate the
  buffered stream; that is expected and refetched silently instead of raising a false alarm.
- **Checksum circuit breaker.** If the OKX checksum fails three times in a row right after fresh
  snapshots, the fault is more likely in the checksum algorithm than in the feed. Verification is
  disabled for that instrument (a `WARN` incident) instead of looping on resubscribes.
- **Lag** = local receive time − venue event time. It needs a synchronised clock (chrony/PTP);
  negative values are counted separately as a clock-skew signal.

## Quick start

```bash
mvn test
mvn package
java -jar target/feedguard-0.1.0-SNAPSHOT.jar \
  --binance BTCUSDT,ETHUSDT,SOLUSDT \
  --okx BTC-USDT,ETH-USDT,SOL-USDT
```

| Option | Default | Meaning |
|---|---|---|
| `--binance` | `BTCUSDT,ETHUSDT,SOLUSDT` | Binance spot symbols, or `none` |
| `--okx` | `BTC-USDT,ETH-USDT,SOL-USDT` | OKX instrument ids, or `none` |
| `--stale-ms` | `5000` | silence threshold per stream |
| `--stats-sec` | `60` | interval for lag statistics |
| `--out` | `incidents.jsonl` | incident log, one JSON object per line |
| `--duration-sec` | `0` (run forever) | stop after N seconds |
| `--raw-out` | — | also write the first raw messages per venue to this file |
| `--raw-count` | `200` | raw messages captured per venue |
| `--reconcile-sec` | `60` | book reconciliation interval, `0` to disable |

Example incident:

```json
{"ts":"2026-10-17T20:50:35.089Z","venue":"OKX","symbol":"ETH-USDT","type":"BOOK_SYNCED","severity":"INFO","detail":"seqId=75287012594, resynced 955 ms after incident"}
```

### Where to run it

Latency figures are only meaningful close to the matching engines. Binance and OKX both run in
Tokyo (AWS `ap-northeast-1`), so a small VM there with chrony enabled gives venue-attributable
numbers. Running from elsewhere is fine for validating detection, but ignore the lag figures.

## Architecture

```
core/          Incident, IncidentBus, console + JSONL sinks, raw capture
json/          zero-dependency JSON parser
book/          OrderBook (keeps raw price/size strings, needed for checksums)
detect/        StalenessMonitor, LatencyStats, SchemaDriftDetector
ws/            ResilientWebSocket (reconnect, serialised sends, heartbeat)
venue/binance  BinanceDepthSync (pure logic) + BinanceCollector
venue/okx      OkxBookSync (pure logic + CRC32) + OkxCollector
```

Sync logic is separated from I/O so it can be tested without a network. The
`*FaultInjectionTest` suites replay simulated streams with dropped messages, corrupted payloads,
crossed books and schema changes.

## Known limitations

- The JSON parser allocates a tree per message: fine for a monitoring probe, not for a hot path.
- Binance REST snapshots use `limit=1000`, which has a request weight; keep symbol counts modest.
- Binance snapshots hold 1000 levels per side. If price drifts far enough, a resting level that
  was beyond that depth at the live book's last rebuild and never updated since can be missing
  from the live book but present in the reference: a `BOOK_MISMATCH` of the "missing in live
  book" kind. It is a real gap in the local book, but one inherent to Binance's protocol rather
  than a feed fault; the detail line tells the kinds apart.
- Each Binance reconciliation round costs one REST depth request per symbol.

## Roadmap

- Bybit, Coinbase, Kraken (CRC32 checksum), Hyperliquid
- Cross-venue divergence (mid vs. median of other venues)
- Adaptive staleness thresholds per symbol and time of day
- Hosted service: incident history, alerting, multi-region probes

## License

[Apache License 2.0](LICENSE)
