package io.feedguard.venue.okx;

import io.feedguard.book.OrderBook;
import io.feedguard.venue.okx.OkxBookSync.Result;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OkxBookSyncTest {

    private static List<String[]> lv(String... pxSz) {
        List<String[]> out = new ArrayList<>();
        for (int i = 0; i < pxSz.length; i += 2) {
            out.add(new String[]{pxSz[i], pxSz[i + 1]});
        }
        return out;
    }

    /** Checksum of the book that results from applying the given levels to a copy. */
    private static int expectedChecksum(List<List<String[]>> bidSteps, List<List<String[]>> askSteps) {
        OrderBook b = new OrderBook();
        bidSteps.forEach(l -> b.applyAll(OrderBook.Side.BID, l));
        askSteps.forEach(l -> b.applyAll(OrderBook.Side.ASK, l));
        return OkxBookSync.checksum(b);
    }

    @Test
    void checksumStringInterleavesBidsAndAsks() {
        // Format described in the OKX API docs (books channel, checksum section).
        OrderBook b = new OrderBook();
        b.applyAll(OrderBook.Side.BID, lv("3366.1", "7", "3366", "6"));
        b.applyAll(OrderBook.Side.ASK, lv("3366.8", "9", "3368", "8"));
        assertEquals("3366.1:7:3366.8:9:3366:6:3368:8", OkxBookSync.checksumInput(b));
    }

    @Test
    void checksumStringWithUnevenSides() {
        OrderBook b = new OrderBook();
        b.applyAll(OrderBook.Side.BID, lv("3366.1", "7"));
        b.applyAll(OrderBook.Side.ASK, lv("3366.8", "9", "3368", "8", "3372", "8"));
        assertEquals("3366.1:7:3366.8:9:3368:8:3372:8", OkxBookSync.checksumInput(b));
    }

    @Test
    void checksumUsesTop25AndRawStrings() {
        OrderBook b = new OrderBook();
        for (int i = 0; i < 30; i++) {
            b.apply(OrderBook.Side.BID, (1000 - i) + ".0", "1");
            b.apply(OrderBook.Side.ASK, (1001 + i) + ".0", "2");
        }
        String input = OkxBookSync.checksumInput(b);
        assertEquals(25 * 4 - 1, input.chars().filter(c -> c == ':').count());
        assertTrue(input.startsWith("1000.0:1:1001.0:2:"));
        assertTrue(input.endsWith("976.0:1:1025.0:2"));

        CRC32 crc = new CRC32();
        crc.update(input.getBytes(StandardCharsets.US_ASCII));
        assertEquals((int) crc.getValue(), OkxBookSync.checksum(b));
    }

    @Test
    void snapshotThenValidUpdates() {
        OkxBookSync s = new OkxBookSync();
        List<String[]> bids = lv("100", "1", "99", "2");
        List<String[]> asks = lv("101", "1", "102", "3");
        assertEquals(Result.SNAPSHOT, s.onSnapshot(10, bids, asks, expectedChecksum(List.of(bids), List.of(asks))));

        List<String[]> bidUpd = lv("100", "0", "99.5", "4"); // delete best bid, add new level
        int cs = expectedChecksum(List.of(bids, bidUpd), List.of(asks));
        assertEquals(Result.APPLIED, s.onUpdate(11, 10, bidUpd, List.of(), cs));
        assertEquals("99.5", s.book().topBids(1).get(0).rawPrice());

        // no-change heartbeat: seqId == prevSeqId, empty levels
        assertEquals(Result.APPLIED, s.onUpdate(11, 11, List.of(), List.of(), cs));
        assertTrue(s.isSynced());
    }

    @Test
    void sequenceGapUnsyncsBook() {
        OkxBookSync s = new OkxBookSync();
        s.onSnapshot(10, lv("100", "1"), lv("101", "1"), null);
        assertEquals(Result.GAP, s.onUpdate(13, 12, List.of(), List.of(), null));
        assertEquals("expected prevSeqId=10 got 12", s.gapDetail());
        assertFalse(s.isSynced());
        assertEquals(Result.NOT_SYNCED, s.onUpdate(14, 13, List.of(), List.of(), null));
        assertEquals(Result.SNAPSHOT, s.onSnapshot(20, lv("100", "1"), lv("101", "1"), null));
    }

    @Test
    void sequenceResetIsAcceptedWhenChainHolds() {
        // OKX may reset seqId lower during maintenance; prevSeqId still chains to our last seqId.
        OkxBookSync s = new OkxBookSync();
        s.onSnapshot(1000, lv("100", "1"), lv("101", "1"), null);
        assertEquals(Result.APPLIED, s.onUpdate(3, 1000, List.of(), List.of(), null));
        assertEquals(Result.APPLIED, s.onUpdate(4, 3, List.of(), List.of(), null));
    }

    @Test
    void checksumMismatchUnsyncsBook() {
        OkxBookSync s = new OkxBookSync();
        List<String[]> bids = lv("100", "1");
        List<String[]> asks = lv("101", "1");
        s.onSnapshot(10, bids, asks, expectedChecksum(List.of(bids), List.of(asks)));
        assertEquals(Result.CHECKSUM_MISMATCH, s.onUpdate(11, 10, lv("100", "2"), List.of(), 12345));
        assertFalse(s.isSynced());
        assertTrue(s.mismatchDetail().startsWith("venue checksum=12345 local="), s.mismatchDetail());
    }

    @Test
    void updatesIgnoredBeforeFirstSnapshot() {
        OkxBookSync s = new OkxBookSync();
        assertEquals(Result.NOT_SYNCED, s.onUpdate(2, 1, lv("100", "1"), List.of(), null));
    }
}
