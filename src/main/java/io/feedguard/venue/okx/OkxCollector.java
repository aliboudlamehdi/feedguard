package io.feedguard.venue.okx;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;
import io.feedguard.detect.LatencyStats;
import io.feedguard.detect.Monitors;
import io.feedguard.detect.StalenessMonitor;
import io.feedguard.json.Json;
import io.feedguard.ws.ResilientWebSocket;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** OKX public "books" channel (400 levels, snapshot + incremental, seqId + CRC32 checksum). */
public final class OkxCollector implements AutoCloseable {

    static final String WS_URL = "wss://ws.okx.com:8443/ws/v5/public";
    static final String CHANNEL = "books";
    /** If the checksum fails this many times in a row right after fresh snapshots, our algorithm is suspect. */
    static final int MAX_CONSECUTIVE_MISMATCHES = 3;

    private final class SymbolState {
        final String instId;
        final OkxBookSync sync = new OkxBookSync();
        final StalenessMonitor.Handle stale;
        final LatencyStats latency;
        boolean crossed;              // guarded by this
        volatile long incidentAtNanos; // 0 = no pending resync
        int consecutiveMismatches;
        boolean checksumEnabled = true;
        boolean checksumZeroReported;

        SymbolState(String instId, Duration staleThreshold) {
            this.instId = instId;
            this.stale = monitors.staleness().register(Venue.OKX, instId, staleThreshold);
            this.latency = monitors.latency(Venue.OKX, instId);
        }
    }

    private final Monitors monitors;
    private final IncidentBus bus;
    private final ScheduledExecutorService scheduler;
    private final Map<String, SymbolState> states = new LinkedHashMap<>();
    private volatile Consumer<String> sender = s -> { };
    private ResilientWebSocket socket;

    public OkxCollector(List<String> instIds, Monitors monitors, Duration staleThreshold,
                        ScheduledExecutorService scheduler) {
        this.monitors = monitors;
        this.bus = monitors.bus();
        this.scheduler = scheduler;
        for (String id : instIds) {
            String inst = id.toUpperCase(Locale.ROOT);
            states.put(inst, new SymbolState(inst, staleThreshold));
        }
    }

    public void start(HttpClient http) {
        socket = new ResilientWebSocket(Venue.OKX, URI.create(WS_URL), http, scheduler, bus,
                this::handleMessage, this::onConnected, "ping", Duration.ofSeconds(20));
        socket.start();
    }

    void onConnected(Consumer<String> sender) {
        this.sender = sender;
        for (SymbolState st : states.values()) {
            st.sync.invalidate();
            synchronized (st) {
                st.crossed = false;
            }
        }
        sender.accept(op("subscribe", states.keySet().stream().toList()));
    }

    void handleMessage(String text) {
        monitors.capture(Venue.OKX, text);
        if ("pong".equals(text)) {
            return;
        }
        Map<String, Object> root = Json.asObject(Json.parse(text));
        if (root == null) {
            return;
        }
        String event = Json.str(root, "event");
        if (event != null) {
            if ("error".equals(event)) {
                bus.emit(Venue.OKX, "*", IncidentType.VENUE_ERROR, Severity.WARN,
                        "code=" + Json.str(root, "code") + " msg=" + Json.str(root, "msg"));
            }
            return; // subscribe / unsubscribe acks
        }
        SymbolState st = states.get(Json.str(Json.obj(root, "arg"), "instId"));
        List<Object> dataArr = Json.arr(root, "data");
        if (st == null || dataArr == null || dataArr.isEmpty()) {
            return;
        }
        Map<String, Object> d = Json.asObject(dataArr.get(0));
        String action = Json.str(root, "action");

        st.stale.touch();
        long ts = Json.lng(d, "ts", -1);
        if (ts > 0) {
            st.latency.record(System.currentTimeMillis() - ts);
        }
        monitors.schema().observe(Venue.OKX, st.instId, CHANNEL + "/" + action, d);

        Integer checksum = readChecksum(st, d);
        long seqId = Json.lng(d, "seqId", -1);
        List<String[]> bids = Json.levels(Json.arr(d, "bids"));
        List<String[]> asks = Json.levels(Json.arr(d, "asks"));

        OkxBookSync.Result r = "snapshot".equals(action)
                ? st.sync.onSnapshot(seqId, bids, asks, checksum)
                : st.sync.onUpdate(seqId, Json.lng(d, "prevSeqId", -1), bids, asks, checksum);

        switch (r) {
            case SNAPSHOT -> {
                st.consecutiveMismatches = 0;
                long at = st.incidentAtNanos;
                st.incidentAtNanos = 0;
                bus.emit(Venue.OKX, st.instId, IncidentType.BOOK_SYNCED, Severity.INFO, "seqId=" + seqId
                        + (at != 0 ? ", resynced " + (System.nanoTime() - at) / 1_000_000 + " ms after incident" : ""));
                checkCrossed(st);
            }
            case APPLIED -> {
                st.consecutiveMismatches = 0;
                checkCrossed(st);
            }
            case GAP -> {
                bus.emit(Venue.OKX, st.instId, IncidentType.SEQUENCE_GAP, Severity.CRITICAL, st.sync.gapDetail());
                resubscribe(st);
            }
            case CHECKSUM_MISMATCH -> onChecksumMismatch(st);
            case NOT_SYNCED -> {
                // waiting for the snapshot that follows our resubscribe
            }
        }
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
    }
}
