package io.feedguard.venue.binance;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.detect.Monitors;
import io.feedguard.testutil.RecordingSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end collector behaviour on a simulated Binance stream with injected faults. */
class BinanceCollectorFaultInjectionTest {

    private RecordingSink sink;
    private ScheduledExecutorService scheduler;
    private final Deque<BinanceCollector.Snapshot> snapshots = new ArrayDeque<>();
    private BinanceCollector collector;
    private Monitors monitors;

    @BeforeEach
    void setUp() {
        sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        monitors = new Monitors(bus, System::nanoTime);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        BinanceCollector.SnapshotSource source = symbol -> {
            BinanceCollector.Snapshot s = snapshots.pollFirst();
            return s == null ? CompletableFuture.failedFuture(new IllegalStateException("no snapshot queued"))
                    : CompletableFuture.completedFuture(s);
        };
        collector = new BinanceCollector(List.of("BTCUSDT"), monitors, Duration.ofSeconds(5), source, scheduler);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private static String depth(long first, long last, String bids, String asks) {
        return "{\"stream\":\"btcusdt@depth@100ms\",\"data\":{\"e\":\"depthUpdate\",\"E\":" + System.currentTimeMillis()
                + ",\"s\":\"BTCUSDT\",\"U\":" + first + ",\"u\":" + last + ",\"b\":" + bids + ",\"a\":" + asks + "}}";
    }

    private static BinanceCollector.Snapshot snap(long lastUpdateId) {
        return new BinanceCollector.Snapshot(lastUpdateId,
                List.<String[]>of(new String[]{"60000.00", "1.5"}), List.<String[]>of(new String[]{"60001.00", "2"}));
    }

    @Test
    void droppedMessageTriggersGapThenAutomaticResync() {
        snapshots.add(snap(100));
        collector.onConnected();
        assertEquals(List.of(IncidentType.BOOK_SYNCED), sink.types());

        collector.handleMessage(depth(99, 105, "[[\"60000.50\",\"1\"]]", "[]"));
        collector.handleMessage(depth(106, 110, "[]", "[]"));
        // message 111..114 is "lost" on the wire
        snapshots.add(snap(116));
        collector.handleMessage(depth(115, 120, "[]", "[]"));

        assertEquals(List.of(IncidentType.BOOK_SYNCED, IncidentType.SEQUENCE_GAP, IncidentType.BOOK_SYNCED),
                sink.types());
        var gap = sink.lastOf(IncidentType.SEQUENCE_GAP);
        assertEquals(Severity.CRITICAL, gap.severity());
        assertEquals("BTCUSDT", gap.symbol());
        assertTrue(gap.detail().startsWith("missing 4 update id(s): expected U=111 got U=115"), gap.detail());
        assertTrue(sink.last().detail().contains("resynced"), sink.last().detail());

        // stream continues normally on the rebuilt book
        collector.handleMessage(depth(121, 125, "[]", "[]"));
        assertEquals(3, sink.incidents.size());
    }

    @Test
    void crossedBookIsReportedOnceAndClears() {
        snapshots.add(snap(100));
        collector.onConnected();
        collector.handleMessage(depth(101, 101, "[]", "[]"));
        collector.handleMessage(depth(102, 102, "[[\"60002.00\",\"1\"]]", "[]")); // bid above best ask
        collector.handleMessage(depth(103, 103, "[[\"60002.50\",\"1\"]]", "[]")); // still crossed: no repeat
        collector.handleMessage(depth(104, 104, "[[\"60002.00\",\"0\"],[\"60002.50\",\"0\"]]", "[]"));

        assertEquals(List.of(IncidentType.BOOK_SYNCED, IncidentType.CROSSED_BOOK, IncidentType.BOOK_UNCROSSED),
                sink.types());
        assertTrue(sink.lastOf(IncidentType.CROSSED_BOOK).detail().contains("bid=60002.00 ask=60001.00"),
                sink.lastOf(IncidentType.CROSSED_BOOK).detail());
    }

    @Test
    void schemaChangeIsFlagged() {
        snapshots.add(snap(100));
        collector.onConnected();
        collector.handleMessage(depth(101, 101, "[]", "[]"));
        String withNewField = "{\"stream\":\"btcusdt@depth@100ms\",\"data\":{\"e\":\"depthUpdate\",\"E\":1,\"T\":2,"
                + "\"s\":\"BTCUSDT\",\"U\":102,\"u\":102,\"b\":[],\"a\":[]}}";
        collector.handleMessage(withNewField);
        assertEquals(IncidentType.SCHEMA_DRIFT, sink.last().type());
        assertTrue(sink.last().detail().contains("added=[T]"), sink.last().detail());
    }

    @Test
    void snapshotFailureIsReportedAndRetried() {
        collector.onConnected(); // no snapshot queued -> failure
        assertEquals(IncidentType.SNAPSHOT_FAILED, sink.last().type());
        assertTrue(sink.last().detail().contains("no snapshot queued"), sink.last().detail());
    }

    @Test
    void reconciliationVerifiesAHealthyBook() {
        snapshots.add(snap(100));
        collector.onConnected();
        collector.handleMessage(depth(101, 101, "[]", "[]"));
        collector.handleMessage(depth(102, 103, "[]", "[]"));

        snapshots.add(snap(103)); // fresh REST snapshot for the reference book
        collector.reconcileAll();
        collector.handleMessage(depth(104, 105, "[]", "[]"));

        assertEquals(List.of(IncidentType.BOOK_SYNCED), sink.types());
        assertEquals(1, monitors.reconciler(io.feedguard.core.Venue.BINANCE, "BTCUSDT").matches());
    }

    @Test
    void reconciliationCatchesSilentlyWrongBookAndRebuildsIt() {
        snapshots.add(snap(100));
        collector.onConnected();
        collector.handleMessage(depth(101, 101, "[]", "[]"));
        collector.handleMessage(depth(102, 103, "[]", "[]"));

        // Sequence ids are perfect, but the live book content is wrong (e.g. an update was
        // mis-applied by the consumer). No gap detector can see this.
        collector.liveSync("BTCUSDT").book().apply(io.feedguard.book.OrderBook.Side.BID, "60000.00", "9");

        snapshots.add(snap(103));
        collector.reconcileAll();
        snapshots.add(snap(110)); // used by the rebuild after the mismatch
        collector.handleMessage(depth(104, 105, "[]", "[]"));

        assertEquals(List.of(IncidentType.BOOK_SYNCED, IncidentType.BOOK_MISMATCH, IncidentType.BOOK_SYNCED),
                sink.types());
        var mismatch = sink.lastOf(IncidentType.BOOK_MISMATCH);
        assertEquals(Severity.CRITICAL, mismatch.severity());
        assertTrue(mismatch.detail().contains("size at 60000.00 live=9 reference=1.5"), mismatch.detail());
        assertEquals(1, monitors.reconciler(io.feedguard.core.Venue.BINANCE, "BTCUSDT").mismatches());
    }

    @Test
    void reconciliationIsInconclusiveWhenSnapshotFails() {
        snapshots.add(snap(100));
        collector.onConnected();
        collector.handleMessage(depth(101, 101, "[]", "[]"));
        collector.reconcileAll(); // nothing queued -> fetch fails
        collector.handleMessage(depth(102, 102, "[]", "[]"));
        assertEquals(List.of(IncidentType.BOOK_SYNCED), sink.types(), "no false alarm");
        assertEquals(1, monitors.reconciler(io.feedguard.core.Venue.BINANCE, "BTCUSDT").inconclusive());
    }

    @Test
    void venueErrorPayloadIsSurfaced() {
        collector.handleMessage("{\"code\":-1121,\"msg\":\"Invalid symbol.\"}");
        assertEquals(IncidentType.VENUE_ERROR, sink.last().type());
    }
}
