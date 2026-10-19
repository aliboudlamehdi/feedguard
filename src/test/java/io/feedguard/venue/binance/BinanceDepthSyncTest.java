package io.feedguard.venue.binance;

import io.feedguard.venue.binance.BinanceDepthSync.DepthEvent;
import io.feedguard.venue.binance.BinanceDepthSync.Result;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinanceDepthSyncTest {

    private static final List<String[]> NONE = List.of();

    private static DepthEvent ev(long first, long last) {
        return new DepthEvent(first, last, 0, NONE, NONE);
    }

    private static DepthEvent ev(long first, long last, String bidPx, String bidSz) {
        return new DepthEvent(first, last, 0, List.<String[]>of(new String[]{bidPx, bidSz}), NONE);
    }

    private static BinanceDepthSync liveAt(long lastUpdateId) {
        BinanceDepthSync s = new BinanceDepthSync();
        s.onSnapshot(lastUpdateId - 5, List.<String[]>of(new String[]{"99", "1"}), List.<String[]>of(new String[]{"101", "1"}));
        assertEquals(Result.APPLIED, s.onEvent(ev(lastUpdateId - 6, lastUpdateId)));
        assertTrue(s.isLive());
        return s;
    }

    @Test
    void buffersUntilSnapshotThenReplaysStraddlingEvent() {
        BinanceDepthSync s = new BinanceDepthSync();
        assertEquals(Result.BUFFERED, s.onEvent(ev(90, 95)));    // fully covered by snapshot
        assertEquals(Result.BUFFERED, s.onEvent(ev(96, 104, "100", "3")));  // straddles lastUpdateId+1 = 101
        assertEquals(Result.BUFFERED, s.onEvent(ev(105, 110)));
        assertEquals(3, s.bufferedEvents());

        assertEquals(Result.APPLIED, s.onSnapshot(100, NONE, NONE));
        assertTrue(s.isLive());
        assertEquals(110, s.lastUpdateId());
        assertEquals(0, s.bufferedEvents());
        assertEquals(new BigDecimal("100"), s.book().bestBid());
    }

    @Test
    void dropsEventsOlderThanSnapshot() {
        BinanceDepthSync s = new BinanceDepthSync();
        s.onSnapshot(100, NONE, NONE);
        assertEquals(Result.STALE_DROPPED, s.onEvent(ev(95, 100)));
        assertFalse(s.isLive());
        assertEquals(Result.APPLIED, s.onEvent(ev(100, 103)));
        assertTrue(s.isLive());
    }

    @Test
    void snapshotOlderThanStreamIsNotAFeedGap() {
        BinanceDepthSync s = new BinanceDepthSync();
        s.onEvent(ev(150, 155));
        assertEquals(Result.SNAPSHOT_TOO_OLD, s.onSnapshot(100, NONE, NONE));
        assertFalse(s.isLive());
        assertEquals(1, s.bufferedEvents(), "offending event must be kept for the next snapshot");
        assertEquals(Result.APPLIED, s.onSnapshot(152, NONE, NONE));
        assertEquals(155, s.lastUpdateId());
    }

    @Test
    void detectsMissingUpdatesOnLiveBook() {
        BinanceDepthSync s = liveAt(105);
        assertEquals(Result.APPLIED, s.onEvent(ev(106, 110)));
        Result r = s.onEvent(ev(113, 115)); // 111 and 112 lost
        assertEquals(Result.GAP, r);
        assertEquals(2, s.lastGap().missing());
        assertEquals(111, s.lastGap().expectedFirstUpdateId());
        assertFalse(s.isLive());
    }

    @Test
    void detectsOverlapAsNegativeGap() {
        BinanceDepthSync s = liveAt(105);
        assertEquals(Result.GAP, s.onEvent(ev(104, 108)));
        assertEquals(-2, s.lastGap().missing());
    }

    @Test
    void resyncsAfterGapUsingBufferedEvents() {
        BinanceDepthSync s = liveAt(105);
        assertEquals(Result.GAP, s.onEvent(ev(110, 112)));
        assertEquals(Result.BUFFERED, s.onEvent(ev(113, 120)));
        assertEquals(2, s.bufferedEvents());
        assertEquals(Result.APPLIED, s.onSnapshot(111, NONE, NONE));
        assertTrue(s.isLive());
        assertEquals(120, s.lastUpdateId());
    }

    @Test
    void gapInsideBufferIsReportedOnReplay() {
        BinanceDepthSync s = new BinanceDepthSync();
        s.onEvent(ev(101, 105));
        s.onEvent(ev(108, 110)); // 106-107 lost while waiting for the snapshot
        assertEquals(Result.GAP, s.onSnapshot(100, NONE, NONE));
        assertEquals(2, s.lastGap().missing());
    }

    @Test
    void resetDiscardsBookAndBuffer() {
        BinanceDepthSync s = liveAt(105);
        s.reset();
        assertFalse(s.isLive());
        assertEquals(Result.BUFFERED, s.onEvent(ev(200, 201)));
        assertEquals(-1, s.lastUpdateId());
    }

    @Test
    void crossedOnlyReportedWhenLive() {
        BinanceDepthSync s = new BinanceDepthSync();
        s.onSnapshot(10, List.<String[]>of(new String[]{"105", "1"}), List.<String[]>of(new String[]{"100", "1"}));
        assertFalse(s.isCrossed(), "not live yet: snapshot alone must not be judged");
        s.onEvent(ev(11, 12));
        assertTrue(s.isCrossed());
    }

    @Test
    void bufferIsBounded() {
        BinanceDepthSync s = new BinanceDepthSync();
        for (int i = 0; i < BinanceDepthSync.MAX_BUFFER + 10; i++) {
            s.onEvent(ev(i * 2L + 1, i * 2L + 2));
        }
        assertEquals(BinanceDepthSync.MAX_BUFFER, s.bufferedEvents());
        assertEquals(10, s.bufferOverflows());
    }
}
