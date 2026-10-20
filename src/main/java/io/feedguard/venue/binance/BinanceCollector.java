package io.feedguard.venue.binance;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;
import io.feedguard.detect.LatencyStats;
import io.feedguard.detect.Monitors;
import io.feedguard.detect.Reconciler;
import io.feedguard.detect.StalenessMonitor;
import io.feedguard.json.Json;
import io.feedguard.ws.ResilientWebSocket;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/** Binance spot: combined diff-depth stream (100 ms) + REST snapshots, one book per symbol. */
public final class BinanceCollector implements AutoCloseable {

    public record Snapshot(long lastUpdateId, List<String[]> bids, List<String[]> asks) {
    }

    @FunctionalInterface
    public interface SnapshotSource {
        CompletableFuture<Snapshot> fetch(String symbol);
    }

    static final String WS_BASE = "wss://stream.binance.com:9443/stream?streams=";
    static final String REST_DEPTH = "https://api.binance.com/api/v3/depth?limit=1000&symbol=";
    private static final long TOO_OLD_RETRY_MS = 250;
    private static final long FAILED_RETRY_MS = 2_000;
    static final int RECONCILE_DEPTH = 20;

    private final class SymbolState {
        final String symbol;
        final BinanceDepthSync sync = new BinanceDepthSync();
        final AtomicBoolean fetching = new AtomicBoolean();
        final StalenessMonitor.Handle stale;
        final LatencyStats latency;
        final Reconciler reconciler;
        boolean crossed;            // guarded by this
        volatile long incidentAtNanos; // 0 = no pending resync after a gap
        /** Reference book rebuilt from a fresh REST snapshot during a reconciliation round. */
        volatile BinanceDepthSync shadow;

        SymbolState(String symbol, Duration staleThreshold) {
            this.symbol = symbol;
            this.stale = monitors.staleness().register(Venue.BINANCE, symbol, staleThreshold);
            this.latency = monitors.latency(Venue.BINANCE, symbol);
            this.reconciler = monitors.reconciler(Venue.BINANCE, symbol);
        }
    }

    private final Monitors monitors;
    private final IncidentBus bus;
    private final SnapshotSource snapshots;
    private final ScheduledExecutorService scheduler;
    private final Map<String, SymbolState> states = new LinkedHashMap<>();
    private ResilientWebSocket socket;

    public BinanceCollector(List<String> symbols, Monitors monitors, Duration staleThreshold,
                            SnapshotSource snapshots, ScheduledExecutorService scheduler) {
        this.monitors = monitors;
        this.bus = monitors.bus();
        this.snapshots = snapshots;
        this.scheduler = scheduler;
        for (String s : symbols) {
            String sym = s.toUpperCase(Locale.ROOT);
            states.put(sym, new SymbolState(sym, staleThreshold));
        }
    }

    public static SnapshotSource httpSnapshots(HttpClient http) {
        return symbol -> http.sendAsync(
                        HttpRequest.newBuilder(URI.create(REST_DEPTH + symbol))
                                .timeout(Duration.ofSeconds(10)).GET().build(),
                        HttpResponse.BodyHandlers.ofString())
                .thenApply(resp -> {
                    if (resp.statusCode() != 200) {
                        String body = resp.body();
                        throw new IllegalStateException("HTTP " + resp.statusCode() + " "
                                + (body.length() > 200 ? body.substring(0, 200) : body));
                    }
                    Map<String, Object> root = Json.asObject(Json.parse(resp.body()));
                    return new Snapshot(Json.lng(root, "lastUpdateId", -1),
                            Json.levels(Json.arr(root, "bids")), Json.levels(Json.arr(root, "asks")));
                });
    }

    public void start(HttpClient http) {
        String streams = states.keySet().stream()
                .map(s -> s.toLowerCase(Locale.ROOT) + "@depth@100ms")
                .collect(Collectors.joining("/"));
        socket = new ResilientWebSocket(Venue.BINANCE, URI.create(WS_BASE + streams), http, scheduler, bus,
                this::handleMessage, sender -> onConnected(), null, null);
        socket.start();
    }

    /** On every (re)connection: drop books and resync from fresh snapshots. */
    void onConnected() {
        for (SymbolState st : states.values()) {
            if (st.shadow != null) {
                abortReconciliation(st);
            }
            st.sync.reset();
            synchronized (st) {
                st.crossed = false;
            }
            requestSnapshot(st, 0);
        }
    }

    void handleMessage(String text) {
        monitors.capture(Venue.BINANCE, text);
        Map<String, Object> root = Json.asObject(Json.parse(text));
        Map<String, Object> data = Json.obj(root, "data");
        if (data == null) {
            if (Json.has(root, "code") || Json.has(root, "msg")) {
                bus.emit(Venue.BINANCE, "*", IncidentType.VENUE_ERROR, Severity.WARN, text);
            }
            return;
        }
        SymbolState st = states.get(Json.str(data, "s"));
        if (st == null) {
            return;
        }
        st.stale.touch();
        long eventTime = Json.lng(data, "E", -1);
        if (eventTime > 0) {
            st.latency.record(System.currentTimeMillis() - eventTime);
        }
        monitors.schema().observe(Venue.BINANCE, st.symbol, "depthUpdate", data);

        BinanceDepthSync.DepthEvent ev = new BinanceDepthSync.DepthEvent(
                Json.lng(data, "U", -1), Json.lng(data, "u", -1), eventTime,
                Json.levels(Json.arr(data, "b")), Json.levels(Json.arr(data, "a")));

        BinanceDepthSync.Result result = st.sync.onEvent(ev);
        reconcileStep(st, ev, result);
        switch (result) {
            case APPLIED -> checkCrossed(st);
            case GAP -> onGap(st);
            case SNAPSHOT_TOO_OLD -> requestSnapshot(st, TOO_OLD_RETRY_MS);
            default -> {
                // BUFFERED / STALE_DROPPED: nothing to report
            }
        }
    }

    // ---------------------------------------------------------------- reconciliation

    /**
     * Starts a reconciliation round on every live book: a reference book is rebuilt from a fresh
     * REST snapshot plus the same stream events, then compared with the live book once both sit
     * at the same update id. Proves the live book content, not just its sequence continuity.
     */
    public void reconcileAll() {
        for (SymbolState st : states.values()) {
            startReconciliation(st);
        }
    }

    /** Expires rounds that could not reach a common update id in time. Call periodically. */
    public void tick() {
        for (SymbolState st : states.values()) {
            if (st.reconciler.expireIfDue()) {
                st.shadow = null;
            }
        }
    }

    private void startReconciliation(SymbolState st) {
        if (!st.sync.isLive() || !st.reconciler.begin()) {
            return;
        }
        BinanceDepthSync shadow = new BinanceDepthSync();
        st.shadow = shadow;
        CompletableFuture<Snapshot> f;
        try {
            f = snapshots.fetch(st.symbol);
        } catch (RuntimeException e) {
            f = CompletableFuture.failedFuture(e);
        }
        f.whenComplete((snap, err) -> {
            if (st.shadow != shadow) {
                return; // round already ended
            }
            if (err != null) {
                abortReconciliation(st);
                return;
            }
            BinanceDepthSync.Result r = shadow.onSnapshot(snap.lastUpdateId(), snap.bids(), snap.asks());
            if (r == BinanceDepthSync.Result.SNAPSHOT_TOO_OLD || r == BinanceDepthSync.Result.GAP) {
                abortReconciliation(st);
            }
        });
    }

    private void reconcileStep(SymbolState st, BinanceDepthSync.DepthEvent ev, BinanceDepthSync.Result live) {
        BinanceDepthSync shadow = st.shadow;
        if (shadow == null) {
            return;
        }
        if (live != BinanceDepthSync.Result.APPLIED && live != BinanceDepthSync.Result.STALE_DROPPED) {
            abortReconciliation(st); // live book is resyncing: nothing meaningful to compare
            return;
        }
        BinanceDepthSync.Result ref = shadow.onEvent(ev);
        if (ref == BinanceDepthSync.Result.GAP || ref == BinanceDepthSync.Result.SNAPSHOT_TOO_OLD) {
            abortReconciliation(st);
            return;
        }
        long liveId = st.sync.lastUpdateId();
        if (shadow.isLive() && shadow.lastUpdateId() == liveId) {
            st.reconciler.offer(Reconciler.Side.LIVE, st.sync.top(RECONCILE_DEPTH));
            Reconciler.Outcome o = st.reconciler.offer(Reconciler.Side.REFERENCE, shadow.top(RECONCILE_DEPTH));
            st.shadow = null;
            if (o != null && !o.match()) {
                onMismatch(st, o);
            }
        }
    }

    private void abortReconciliation(SymbolState st) {
        st.shadow = null;
        st.reconciler.abort();
    }

    private void onMismatch(SymbolState st, Reconciler.Outcome o) {
        bus.emit(Venue.BINANCE, st.symbol, IncidentType.BOOK_MISMATCH, Severity.CRITICAL,
                "lastUpdateId=" + o.seq() + " " + o.detail() + " - rebuilding book");
        if (st.incidentAtNanos == 0) {
            st.incidentAtNanos = System.nanoTime();
        }
        st.sync.reset();
        synchronized (st) {
            st.crossed = false;
        }
        requestSnapshot(st, 0);
    }

    /** For tests: the live sync of a symbol. */
    BinanceDepthSync liveSync(String symbol) {
        return states.get(symbol).sync;
    }

    // ---------------------------------------------------------------- gaps and snapshots

    private void onGap(SymbolState st) {
        BinanceDepthSync.Gap g = st.sync.lastGap();
        if (st.incidentAtNanos == 0) {
            st.incidentAtNanos = System.nanoTime();
        }
        String detail = g.missing() > 0
                ? "missing " + g.missing() + " update id(s): expected U=" + g.expectedFirstUpdateId()
                  + " got U=" + g.receivedFirstUpdateId()
                : "overlap/out-of-order: expected U=" + g.expectedFirstUpdateId()
                  + " got U=" + g.receivedFirstUpdateId();
        bus.emit(Venue.BINANCE, st.symbol, IncidentType.SEQUENCE_GAP, Severity.CRITICAL, detail);
        requestSnapshot(st, 0);
    }

    private void requestSnapshot(SymbolState st, long delayMs) {
        if (!st.fetching.compareAndSet(false, true)) {
            return;
        }
        Runnable task = () -> {
            CompletableFuture<Snapshot> f;
            try {
                f = snapshots.fetch(st.symbol);
            } catch (RuntimeException e) {
                f = CompletableFuture.failedFuture(e);
            }
            f.whenComplete((snap, err) -> onSnapshot(st, snap, err));
        };
        if (delayMs <= 0) {
            task.run();
        } else {
            scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        }
    }

    private void onSnapshot(SymbolState st, Snapshot snap, Throwable err) {
        st.fetching.set(false);
        if (err != null) {
            bus.emit(Venue.BINANCE, st.symbol, IncidentType.SNAPSHOT_FAILED, Severity.WARN, rootMessage(err));
            requestSnapshot(st, FAILED_RETRY_MS);
            return;
        }
        switch (st.sync.onSnapshot(snap.lastUpdateId(), snap.bids(), snap.asks())) {
            case SNAPSHOT_TOO_OLD -> requestSnapshot(st, TOO_OLD_RETRY_MS);
            case GAP -> onGap(st);
            default -> {
                long at = st.incidentAtNanos;
                st.incidentAtNanos = 0;
                String detail = "lastUpdateId=" + snap.lastUpdateId()
                        + (at != 0 ? ", resynced " + (System.nanoTime() - at) / 1_000_000 + " ms after gap" : "");
                bus.emit(Venue.BINANCE, st.symbol, IncidentType.BOOK_SYNCED, Severity.INFO, detail);
                checkCrossed(st);
            }
        }
    }

    private void checkCrossed(SymbolState st) {
        boolean crossed = st.sync.isCrossed();
        synchronized (st) {
            if (crossed && !st.crossed) {
                bus.emit(Venue.BINANCE, st.symbol, IncidentType.CROSSED_BOOK, Severity.CRITICAL,
                        st.sync.topOfBook() + " lastUpdateId=" + st.sync.lastUpdateId());
            } else if (!crossed && st.crossed) {
                bus.emit(Venue.BINANCE, st.symbol, IncidentType.BOOK_UNCROSSED, Severity.INFO, st.sync.topOfBook());
            }
            st.crossed = crossed;
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getClass().getSimpleName() + (c.getMessage() == null ? "" : ": " + c.getMessage());
    }

    @Override
    public void close() {
        if (socket != null) {
            socket.close();
        }
    }
}
