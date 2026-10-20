package io.feedguard.venue.okx;

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
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * OKX public "books" channel (400 levels, snapshot + incremental, seqId/prevSeqId chaining).
 *
 * <p>Reconciliation: OKX's REST order book carries no seqId, so it cannot be aligned with the
 * stream. Instead a short-lived second WebSocket subscribes to the same instruments; its fresh
 * snapshot plus updates form a reference book, compared with the live book at an equal seqId.
 */
public final class OkxCollector implements AutoCloseable {

    static final String WS_URL = "wss://ws.okx.com:8443/ws/v5/public";
    static final String CHANNEL = "books";
    /** If the checksum fails this many times in a row right after fresh snapshots, our algorithm is suspect. */
    static final int MAX_CONSECUTIVE_MISMATCHES = 3;
    static final int RECONCILE_DEPTH = 20;

    /** Opens a reference connection subscribed to the given instruments; returns its closer. */
    @FunctionalInterface
    public interface ShadowOpener {
        AutoCloseable open(List<String> instIds, Consumer<String> onMessage);
    }

    private record BookMessage(String instId, String action, Map<String, Object> data, long seqId, long prevSeqId,
                               List<String[]> bids, List<String[]> asks) {
        boolean isSnapshot() {
            return "snapshot".equals(action);
        }
    }

    private final class SymbolState {
        final String instId;
        final OkxBookSync sync = new OkxBookSync();
        final StalenessMonitor.Handle stale;
        final LatencyStats latency;
        final Reconciler reconciler;
        boolean crossed;              // guarded by this
        volatile long incidentAtNanos; // 0 = no pending resync
        int consecutiveMismatches;
        boolean checksumEnabled = true;
        boolean checksumZeroReported;
        /** Reference book fed by the shadow connection during a reconciliation round. */
        volatile OkxBookSync shadow;

        SymbolState(String instId, Duration staleThreshold) {
            this.instId = instId;
            this.stale = monitors.staleness().register(Venue.OKX, instId, staleThreshold);
            this.latency = monitors.latency(Venue.OKX, instId);
            this.reconciler = monitors.reconciler(Venue.OKX, instId);
        }
    }

    private final Monitors monitors;
    private final IncidentBus bus;
    private final ScheduledExecutorService scheduler;
    private final Map<String, SymbolState> states = new LinkedHashMap<>();
    private volatile Consumer<String> sender = s -> { };
    private volatile ShadowOpener shadowOpener;
    private AutoCloseable shadowConnection; // guarded by this
    private ResilientWebSocket socket;

    public OkxCollector(List<String> instIds, Monitors monitors, Duration staleThreshold,
                        ScheduledExecutorService scheduler) {
        this(instIds, monitors, staleThreshold, scheduler, null);
    }

    OkxCollector(List<String> instIds, Monitors monitors, Duration staleThreshold,
                 ScheduledExecutorService scheduler, ShadowOpener shadowOpener) {
        this.monitors = monitors;
        this.bus = monitors.bus();
        this.scheduler = scheduler;
        this.shadowOpener = shadowOpener;
        for (String id : instIds) {
            String inst = id.toUpperCase(Locale.ROOT);
            states.put(inst, new SymbolState(inst, staleThreshold));
        }
    }

    public void start(HttpClient http) {
        if (shadowOpener == null) {
            shadowOpener = (ids, onMessage) -> openShadowSocket(http, ids, onMessage);
        }
        socket = new ResilientWebSocket(Venue.OKX, URI.create(WS_URL), http, scheduler, bus,
                this::handleMessage, this::onConnected, "ping", Duration.ofSeconds(20));
        socket.start();
    }

    void onConnected(Consumer<String> sender) {
        this.sender = sender;
        for (SymbolState st : states.values()) {
            abortReconciliation(st);
            st.sync.invalidate();
            synchronized (st) {
                st.crossed = false;
            }
        }
        sender.accept(op("subscribe", states.keySet().stream().toList()));
    }

    // ---------------------------------------------------------------- live stream

    void handleMessage(String text) {
        monitors.capture(Venue.OKX, text);
        BookMessage m = parse(text, true);
        if (m == null) {
            return;
        }
        SymbolState st = states.get(m.instId());
        if (st == null) {
            return;
        }
        st.stale.touch();
        long ts = Json.lng(m.data(), "ts", -1);
        if (ts > 0) {
            st.latency.record(System.currentTimeMillis() - ts);
        }
        monitors.schema().observe(Venue.OKX, st.instId, CHANNEL + "/" + m.action(), m.data());

        Integer checksum = readChecksum(st, m.data());
        OkxBookSync.Result r = m.isSnapshot()
                ? st.sync.onSnapshot(m.seqId(), m.bids(), m.asks(), checksum)
                : st.sync.onUpdate(m.seqId(), m.prevSeqId(), m.bids(), m.asks(), checksum);

        switch (r) {
            case SNAPSHOT -> {
                st.consecutiveMismatches = 0;
                long at = st.incidentAtNanos;
                st.incidentAtNanos = 0;
                bus.emit(Venue.OKX, st.instId, IncidentType.BOOK_SYNCED, Severity.INFO, "seqId=" + m.seqId()
                        + (at != 0 ? ", resynced " + (System.nanoTime() - at) / 1_000_000 + " ms after incident" : ""));
                checkCrossed(st);
                offer(st, Reconciler.Side.LIVE);
            }
            case APPLIED -> {
                st.consecutiveMismatches = 0;
                checkCrossed(st);
                offer(st, Reconciler.Side.LIVE);
            }
            case GAP -> {
                abortReconciliation(st);
                bus.emit(Venue.OKX, st.instId, IncidentType.SEQUENCE_GAP, Severity.CRITICAL, st.sync.gapDetail());
                resubscribe(st);
            }
            case CHECKSUM_MISMATCH -> {
                abortReconciliation(st);
                onChecksumMismatch(st);
            }
            case NOT_SYNCED -> {
                // waiting for the snapshot that follows our resubscribe
            }
        }
    }

    /** Parses a books push; returns null for pongs, acks and other non-book messages. */
    private BookMessage parse(String text, boolean reportErrors) {
        if ("pong".equals(text)) {
            return null;
        }
        Map<String, Object> root = Json.asObject(Json.parse(text));
        if (root == null) {
            return null;
        }
        String event = Json.str(root, "event");
        if (event != null) {
            if (reportErrors && "error".equals(event)) {
                bus.emit(Venue.OKX, "*", IncidentType.VENUE_ERROR, Severity.WARN,
                        "code=" + Json.str(root, "code") + " msg=" + Json.str(root, "msg"));
            }
            return null; // subscribe / unsubscribe acks
        }
        List<Object> dataArr = Json.arr(root, "data");
        String instId = Json.str(Json.obj(root, "arg"), "instId");
        if (instId == null || dataArr == null || dataArr.isEmpty()) {
            return null;
        }
        Map<String, Object> d = Json.asObject(dataArr.get(0));
        return new BookMessage(instId, Json.str(root, "action"), d,
                Json.lng(d, "seqId", -1), Json.lng(d, "prevSeqId", -1),
                Json.levels(Json.arr(d, "bids")), Json.levels(Json.arr(d, "asks")));
    }

    /**
     * Returns the checksum to verify, or null when there is nothing meaningful to check.
     * OKX deprecated the field on books / books-l2-tbt / books50-l2-tbt (production: 23 June 2026):
     * it is still sent but always 0, and seqId/prevSeqId must be used instead. A genuine CRC32
     * equals 0 with probability 2^-32, so 0 is treated as "not provided".
     * https://www.okx.com/en-gb/help/okx-order-book-channels-checksum-field-deprecation
     */
    private Integer readChecksum(SymbolState st, Map<String, Object> d) {
        if (!st.checksumEnabled || !Json.has(d, "checksum")) {
            return null;
        }
        int value = (int) Json.lng(d, "checksum", 0);
        if (value == 0) {
            if (!st.checksumZeroReported) {
                st.checksumZeroReported = true;
                bus.emit(Venue.OKX, st.instId, IncidentType.CHECKSUM_UNAVAILABLE, Severity.INFO,
                        "venue sends checksum=0 (deprecated by OKX); book integrity relies on seqId chaining only");
            }
            return null;
        }
        return value;
    }

    private void onChecksumMismatch(SymbolState st) {
        st.consecutiveMismatches++;
        if (st.consecutiveMismatches >= MAX_CONSECUTIVE_MISMATCHES) {
            // Persistent mismatch straight after fresh snapshots points at our checksum
            // implementation (or a venue format change), not at the feed: stop the resubscribe loop.
            st.checksumEnabled = false;
            bus.emit(Venue.OKX, st.instId, IncidentType.CHECKSUM_MISMATCH, Severity.WARN,
                    st.sync.mismatchDetail() + " - " + MAX_CONSECUTIVE_MISMATCHES
                            + " consecutive mismatches, checksum verification disabled for this instrument;"
                            + " verify the algorithm against current OKX docs");
        } else {
            bus.emit(Venue.OKX, st.instId, IncidentType.CHECKSUM_MISMATCH, Severity.CRITICAL,
                    st.sync.mismatchDetail());
        }
        resubscribe(st);
    }

    private void resubscribe(SymbolState st) {
        if (st.incidentAtNanos == 0) {
            st.incidentAtNanos = System.nanoTime();
        }
        sender.accept(op("unsubscribe", List.of(st.instId)));
        sender.accept(op("subscribe", List.of(st.instId)));
    }

    private void checkCrossed(SymbolState st) {
        boolean crossed = st.sync.isCrossed();
        synchronized (st) {
            if (crossed && !st.crossed) {
                bus.emit(Venue.OKX, st.instId, IncidentType.CROSSED_BOOK, Severity.CRITICAL,
                        st.sync.topOfBook() + " seqId=" + st.sync.lastSeqId());
            } else if (!crossed && st.crossed) {
                bus.emit(Venue.OKX, st.instId, IncidentType.BOOK_UNCROSSED, Severity.INFO, st.sync.topOfBook());
            }
            st.crossed = crossed;
        }
    }

    // ---------------------------------------------------------------- reconciliation

    /** Starts a reconciliation round for every synced book over one shared shadow connection. */
    public void reconcileAll() {
        List<String> ids = new ArrayList<>();
        synchronized (this) {
            if (shadowConnection != null || shadowOpener == null) {
                return; // previous round still running, or not started
            }
            for (SymbolState st : states.values()) {
                if (st.sync.isSynced() && st.reconciler.begin()) {
                    st.shadow = new OkxBookSync();
                    ids.add(st.instId);
                }
            }
            if (ids.isEmpty()) {
                return;
            }
            try {
                shadowConnection = shadowOpener.open(ids, this::handleShadowMessage);
            } catch (RuntimeException e) {
                for (SymbolState st : states.values()) {
                    abortReconciliation(st);
                }
            }
        }
    }

    /** Expires rounds that could not reach a common seqId in time. Call periodically. */
    public void tick() {
        for (SymbolState st : states.values()) {
            if (st.reconciler.expireIfDue()) {
                st.shadow = null;
            }
        }
        closeShadowIfIdle();
    }

    void handleShadowMessage(String text) {
        BookMessage m = parse(text, false);
        if (m == null) {
            return;
        }
        SymbolState st = states.get(m.instId());
        OkxBookSync shadow = st == null ? null : st.shadow;
        if (shadow == null) {
            return;
        }
        OkxBookSync.Result r = m.isSnapshot()
                ? shadow.onSnapshot(m.seqId(), m.bids(), m.asks(), null)
                : shadow.onUpdate(m.seqId(), m.prevSeqId(), m.bids(), m.asks(), null);
        if (r == OkxBookSync.Result.SNAPSHOT || r == OkxBookSync.Result.APPLIED) {
            offer(st, Reconciler.Side.REFERENCE);
        } else if (r == OkxBookSync.Result.GAP) {
            abortReconciliation(st); // the reference itself lost data: inconclusive
            closeShadowIfIdle();
        }
    }

    private void offer(SymbolState st, Reconciler.Side side) {
        OkxBookSync book = side == Reconciler.Side.LIVE ? st.sync : st.shadow;
        if (st.shadow == null || book == null) {
            return;
        }
        Reconciler.Outcome o = st.reconciler.offer(side, book.top(RECONCILE_DEPTH));
        if (o == null) {
            return;
        }
        st.shadow = null;
        if (!o.match()) {
            bus.emit(Venue.OKX, st.instId, IncidentType.BOOK_MISMATCH, Severity.CRITICAL,
                    "seqId=" + o.seq() + " " + o.detail() + " - rebuilding book");
            st.sync.invalidate();
            resubscribe(st);
        }
        closeShadowIfIdle();
    }

    /** For tests: the live sync of an instrument. */
    OkxBookSync liveSync(String instId) {
        return states.get(instId).sync;
    }

    private void abortReconciliation(SymbolState st) {
        st.shadow = null;
        st.reconciler.abort();
    }

    private void closeShadowIfIdle() {
        AutoCloseable toClose = null;
        synchronized (this) {
            if (shadowConnection != null && states.values().stream().allMatch(s -> s.shadow == null)) {
                toClose = shadowConnection;
                shadowConnection = null;
            }
        }
        if (toClose != null) {
            try {
                toClose.close();
            } catch (Exception ignored) {
                // best effort
            }
        }
    }

    /** Plain, non-reconnecting WebSocket: a round is short and simply ends inconclusive on failure. */
    private static AutoCloseable openShadowSocket(HttpClient http, List<String> instIds, Consumer<String> onMessage) {
        StringBuilder partial = new StringBuilder();
        WebSocket.Listener listener = new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket ws) {
                ws.sendText(op("subscribe", instIds), true);
                ws.request(1);
            }

            @Override
            public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                partial.append(data);
                if (last) {
                    String msg = partial.toString();
                    partial.setLength(0);
                    try {
                        onMessage.accept(msg);
                    } catch (RuntimeException ignored) {
                        // a malformed reference message only makes the round inconclusive
                    }
                }
                ws.request(1);
                return null;
            }
        };
        var future = http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(URI.create(WS_URL), listener);
        return () -> future.thenAccept(ws -> ws.sendClose(WebSocket.NORMAL_CLOSURE, "reconciled"));
    }

    static String op(String op, List<String> instIds) {
        return "{\"op\":\"" + op + "\",\"args\":[" + instIds.stream()
                .map(id -> "{\"channel\":\"" + CHANNEL + "\",\"instId\":\"" + id + "\"}")
                .collect(Collectors.joining(",")) + "]}";
    }

    @Override
    public void close() {
        if (socket != null) {
            socket.close();
        }
        for (SymbolState st : states.values()) {
            st.shadow = null;
        }
        closeShadowIfIdle();
    }
}
