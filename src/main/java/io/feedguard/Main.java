package io.feedguard;

import io.feedguard.core.ConsoleSink;
import io.feedguard.core.IncidentBus;
import io.feedguard.core.JsonlFileSink;
import io.feedguard.core.RawCapture;
import io.feedguard.detect.LatencyStats;
import io.feedguard.detect.Monitors;
import io.feedguard.detect.Reconciler;
import io.feedguard.venue.binance.BinanceCollector;
import io.feedguard.venue.okx.OkxCollector;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * FeedGuard probe.
 *
 * <pre>
 * java -jar feedguard.jar [--binance BTCUSDT,ETHUSDT|none] [--okx BTC-USDT,ETH-USDT|none]
 *                         [--stale-ms 5000] [--stats-sec 60] [--out incidents.jsonl] [--duration-sec 0]
 *                         [--raw-out raw.log] [--raw-count 200] [--reconcile-sec 60]
 * </pre>
 */
public final class Main {

    public static void main(String[] args) throws InterruptedException {
        Map<String, String> opts = parse(args);
        List<String> binanceSymbols = list(opts.getOrDefault("binance", "BTCUSDT,ETHUSDT,SOLUSDT"));
        List<String> okxSymbols = list(opts.getOrDefault("okx", "BTC-USDT,ETH-USDT,SOL-USDT"));
        Duration stale = Duration.ofMillis(Long.parseLong(opts.getOrDefault("stale-ms", "5000")));
        long statsSec = Long.parseLong(opts.getOrDefault("stats-sec", "60"));
        Path out = Path.of(opts.getOrDefault("out", "incidents.jsonl"));
        long durationSec = Long.parseLong(opts.getOrDefault("duration-sec", "0"));
        long reconcileSec = Long.parseLong(opts.getOrDefault("reconcile-sec", "60"));

        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(new ConsoleSink());
        bus.addSink(new JsonlFileSink(out));
        Monitors monitors = new Monitors(bus, System::nanoTime);
        RawCapture raw = null;
        if (opts.containsKey("raw-out")) {
            int rawCount = Integer.parseInt(opts.getOrDefault("raw-count", "200"));
            raw = new RawCapture(Path.of(opts.get("raw-out")), rawCount);
            monitors.setRawCapture(raw);
            System.out.printf("Capturing first %d raw messages per venue to %s%n",
                    rawCount, Path.of(opts.get("raw-out")).toAbsolutePath());
        }
        RawCapture rawCapture = raw;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "feedguard-scheduler");
            t.setDaemon(true);
            return t;
        });
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

        BinanceCollector binance = binanceSymbols.isEmpty() ? null
                : new BinanceCollector(binanceSymbols, monitors, stale, BinanceCollector.httpSnapshots(http), scheduler);
        OkxCollector okx = okxSymbols.isEmpty() ? null
                : new OkxCollector(okxSymbols, monitors, stale, scheduler);

        System.out.printf("FeedGuard probe - binance=%s okx=%s stale=%dms stats=%ds reconcile=%s incidents=%s%n",
                binanceSymbols, okxSymbols, stale.toMillis(), statsSec,
                reconcileSec > 0 ? reconcileSec + "s" : "off", out.toAbsolutePath());

        if (binance != null) {
            binance.start(http);
        }
        if (okx != null) {
            okx.start(http);
        }
        scheduler.scheduleAtFixedRate(monitors.staleness()::check, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(() -> printStats(monitors), statsSec, statsSec, TimeUnit.SECONDS);
        if (reconcileSec > 0) {
            scheduler.scheduleAtFixedRate(() -> {
                if (binance != null) {
                    binance.reconcileAll();
                }
                if (okx != null) {
                    okx.reconcileAll();
                }
            }, reconcileSec, reconcileSec, TimeUnit.SECONDS);
            scheduler.scheduleAtFixedRate(() -> {
                if (binance != null) {
                    binance.tick();
                }
                if (okx != null) {
                    okx.tick();
                }
            }, 1, 1, TimeUnit.SECONDS);
        }

        CountDownLatch done = new CountDownLatch(1);
        Thread shutdown = new Thread(() -> {
            // Stop periodic checks first so nothing writes to the sinks after they are closed.
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (binance != null) {
                binance.close();
            }
            if (okx != null) {
                okx.close();
            }
            printStats(monitors);
            System.out.println("Incident counts: " + bus.counts());
            bus.closeSinks();
            if (rawCapture != null) {
                rawCapture.close();
            }
            done.countDown();
        }, "feedguard-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);

        if (durationSec > 0) {
            if (!done.await(durationSec, TimeUnit.SECONDS)) {
                System.exit(0); // runs the shutdown hook
            }
        } else {
            done.await();
        }
    }

    private static void printStats(Monitors monitors) {
        for (Map.Entry<String, LatencyStats> e : monitors.allLatency().entrySet()) {
            LatencyStats.Snapshot s = e.getValue().snapshotAndReset();
            System.out.printf("[stats] %-20s msgs=%-6d lag p50=%dms p99=%dms max=%dms%s%n",
                    e.getKey(), s.count(), s.p50(), s.p99(), s.max(),
                    s.negativeCount() > 0 ? " (negative=" + s.negativeCount() + ": check clock sync)" : "");
        }
        for (Map.Entry<String, Reconciler> e : monitors.allReconcilers().entrySet()) {
            System.out.printf("[recon] %-20s %s%n", e.getKey(), e.getValue().summary());
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected --option value pairs, got: " + Arrays.toString(args));
            }
            m.put(args[i].substring(2), args[++i]);
        }
        return m;
    }

    private static List<String> list(String csv) {
        if (csv.isBlank() || csv.equalsIgnoreCase("none")) {
            return List.of();
        }
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }
}
