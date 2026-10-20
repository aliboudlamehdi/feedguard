package io.feedguard.venue.okx;

import io.feedguard.book.OrderBook;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end collector behaviour on a simulated OKX stream with injected faults. */
class OkxCollectorFaultInjectionTest {

    private RecordingSink sink;
    private ScheduledExecutorService scheduler;
    private OkxCollector collector;
    private Monitors monitors;
    private final List<String> sent = new ArrayList<>();
    /** Mirror of what the venue's book should be, used to produce correct checksums. */
    private final OrderBook venueBook = new OrderBook();

    @BeforeEach
    void setUp() {
        sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        scheduler = Executors.newSingleThreadScheduledExecutor();
        monitors = new Monitors(bus, System::nanoTime);
        collector = new OkxCollector(List.of("BTC-USDT"), monitors, Duration.ofSeconds(5), scheduler);
        collector.onConnected(sent::add);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private String msg(String action, long seqId, long prevSeqId, String bids, String asks, Integer checksumOverride) {
        int checksum = checksumOverride != null ? checksumOverride : OkxBookSync.checksum(venueBook);
        return "{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"" + action + "\",\"data\":[{"
                + "\"asks\":" + asks + ",\"bids\":" + bids + ",\"ts\":\"" + System.currentTimeMillis() + "\","
                + "\"checksum\":" + checksum + ",\"prevSeqId\":" + prevSeqId + ",\"seqId\":" + seqId + "}]}";
    }

    private String snapshot(long seqId, Integer checksumOverride) {
        venueBook.clear();
        venueBook.apply(OrderBook.Side.BID, "60000.1", "1.5");
        venueBook.apply(OrderBook.Side.ASK, "60000.2", "0.8");
        return msg("snapshot", seqId, -1,
                "[[\"60000.1\",\"1.5\",\"0\",\"3\"]]", "[[\"60000.2\",\"0.8\",\"0\",\"2\"]]", checksumOverride);
    }

    private String bidUpdate(long seqId, long prevSeqId, String px, String sz, Integer checksumOverride) {
        venueBook.apply(OrderBook.Side.BID, px, sz);
        return msg("update", seqId, prevSeqId, "[[\"" + px + "\",\"" + sz + "\",\"0\",\"1\"]]", "[]", checksumOverride);
    }

    @Test
    void subscribesOnConnect() {
        assertEquals(List.of("{\"op\":\"subscribe\",\"args\":[{\"channel\":\"books\",\"instId\":\"BTC-USDT\"}]}"), sent);
    }

    @Test
    void sequenceGapTriggersResubscribeAndResync() {
        collector.handleMessage(snapshot(10, null));
        collector.handleMessage(bidUpdate(11, 10, "60000.0", "2", null));
        sent.clear();
        collector.handleMessage(bidUpdate(13, 12, "59999.9", "1", null)); // seqId 12 lost
        collector.handleMessage(bidUpdate(14, 13, "59999.8", "1", null)); // ignored until snapshot

        assertEquals(List.of(IncidentType.BOOK_SYNCED, IncidentType.SEQUENCE_GAP), sink.types());
        assertEquals("expected prevSeqId=11 got 12", sink.last().detail());
        assertEquals(2, sent.size());
        assertTrue(sent.get(0).contains("\"op\":\"unsubscribe\""));
        assertTrue(sent.get(1).contains("\"op\":\"subscribe\""));

        collector.handleMessage(snapshot(20, null));
        assertEquals(IncidentType.BOOK_SYNCED, sink.last().type());
        assertTrue(sink.last().detail().contains("resynced"), sink.last().detail());
    }

    @Test
    void corruptedUpdateIsCaughtByChecksum() {
        collector.handleMessage(snapshot(10, null));
        sent.clear();
        // The venue's book changed but the message we receive carries a different size: simulate a
        // corrupted payload by sending a checksum that does not match what we will compute.
        collector.handleMessage(bidUpdate(11, 10, "60000.0", "2", 424242));
        assertEquals(IncidentType.CHECKSUM_MISMATCH, sink.last().type());
        assertEquals(Severity.CRITICAL, sink.last().severity());
        assertEquals(2, sent.size());
    }

    @Test
    void persistentMismatchDisablesChecksumInsteadOfLooping() {
        for (int i = 0; i < OkxCollector.MAX_CONSECUTIVE_MISMATCHES; i++) {
            collector.handleMessage(snapshot(10 + i, 1));
        }
        var last = sink.lastOf(IncidentType.CHECKSUM_MISMATCH);
        assertEquals(Severity.WARN, last.severity());
        assertTrue(last.detail().contains("verification disabled"), last.detail());

        collector.handleMessage(snapshot(50, 1)); // bad checksum now ignored
        assertEquals(IncidentType.BOOK_SYNCED, sink.last().type());
    }

    @Test
    void zeroChecksumMeansNotProvidedAndIsReportedOnce() {
        // OKX deprecated the checksum (June 2026): always "checksum":0.
        collector.handleMessage(snapshot(10, 0));
        collector.handleMessage(bidUpdate(11, 10, "60000.0", "2", 0));
        collector.handleMessage(bidUpdate(12, 11, "59999.0", "1", 0));
        assertEquals(List.of(IncidentType.CHECKSUM_UNAVAILABLE, IncidentType.BOOK_SYNCED), sink.types());
        assertEquals(Severity.INFO, sink.incidents.get(0).severity());

        // sequence checking still works without checksums
        collector.handleMessage(bidUpdate(15, 14, "59998.0", "1", 0));
        assertEquals(IncidentType.SEQUENCE_GAP, sink.last().type());
    }

    /** Snapshot message reflecting the current venue book. */
    private String venueSnapshot(long seqId) {
        return msg("snapshot", seqId, -1, levelsJson(venueBook.topBids(400)), levelsJson(venueBook.topAsks(400)), 0);
    }

    private static String levelsJson(List<OrderBook.Level> levels) {
        StringBuilder sb = new StringBuilder("[");
        for (OrderBook.Level l : levels) {
            if (sb.length() > 1) {
                sb.append(',');
            }
            sb.append("[\"").append(l.rawPrice()).append("\",\"").append(l.rawSize()).append("\",\"0\",\"1\"]");
        }
        return sb.append(']').toString();
    }

    @Test
    void reconciliationOverShadowSubscriptionVerifiesHealthyBook() {
        List<List<String>> opened = new ArrayList<>();
        boolean[] closed = {false};
        OkxCollector c = new OkxCollector(List.of("BTC-USDT"), monitors, Duration.ofSeconds(5), scheduler,
                (ids, onMessage) -> {
                    opened.add(ids);
                    return () -> closed[0] = true;
                });
        c.onConnected(sent::add);
        c.handleMessage(snapshot(10, null));
        c.reconcileAll();
        assertEquals(List.of(List.of("BTC-USDT")), opened);

        c.handleMessage(bidUpdate(11, 10, "60000.0", "2", null)); // live at seqId 11
        c.handleShadowMessage(venueSnapshot(11));                  // reference snapshot at seqId 11

        assertEquals(1, monitors.reconciler(io.feedguard.core.Venue.OKX, "BTC-USDT").matches());
        assertTrue(closed[0], "shadow connection closed once all rounds are done");
        assertTrue(sink.types().stream().noneMatch(t -> t == IncidentType.BOOK_MISMATCH));
    }

    @Test
    void reconciliationCatchesSilentlyWrongBook() {
        OkxCollector c = new OkxCollector(List.of("BTC-USDT"), monitors, Duration.ofSeconds(5), scheduler,
                (ids, onMessage) -> () -> { });
        c.onConnected(sent::add);
        c.handleMessage(snapshot(10, null));
        c.liveSync("BTC-USDT").book().apply(OrderBook.Side.ASK, "60000.5", "3"); // phantom level
        c.reconcileAll();
        sent.clear();

        c.handleShadowMessage(venueSnapshot(10)); // reference first this time
        // checksum 0, as OKX sends since June 2026: the checksum can no longer catch this
        String update = bidUpdate(11, 10, "60000.0", "2", 0);
        c.handleMessage(update);
        c.handleShadowMessage(update); // both connections receive the same push

        var mismatch = sink.lastOf(IncidentType.BOOK_MISMATCH);
        assertTrue(mismatch.detail().contains("extra in live book 60000.5x3"), mismatch.detail());
        assertEquals(2, sent.size(), "live book resubscribed");
        assertEquals(1, monitors.reconciler(io.feedguard.core.Venue.OKX, "BTC-USDT").mismatches());
    }

    @Test
    void crossedBookDetected() {
        collector.handleMessage(snapshot(10, null));
        collector.handleMessage(bidUpdate(11, 10, "60000.3", "1", null)); // bid above ask 60000.2
        assertEquals(IncidentType.CROSSED_BOOK, sink.last().type());
    }

    @Test
    void venueErrorAndAcksAndPong() {
        collector.handleMessage("pong");
        collector.handleMessage("{\"event\":\"subscribe\",\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"}}");
        assertTrue(sink.incidents.isEmpty());
        collector.handleMessage("{\"event\":\"error\",\"code\":\"60018\",\"msg\":\"Wrong URL or channel\"}");
        assertEquals(IncidentType.VENUE_ERROR, sink.last().type());
        assertTrue(sink.last().detail().contains("60018"));
    }
}
