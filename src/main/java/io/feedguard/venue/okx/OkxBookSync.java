package io.feedguard.venue.okx;

import io.feedguard.book.OrderBook;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * OKX "books" channel synchronisation with two independent integrity checks:
 *
 * <ul>
 *   <li>Sequence: each update's {@code prevSeqId} must equal the previous message's
 *       {@code seqId}. A no-change heartbeat has {@code seqId == prevSeqId}, which passes.</li>
 *   <li>Checksum: CRC32 (signed 32-bit) of the top 25 levels, interleaved
 *       {@code bidPx:bidSz:askPx:askSz:...}; when one side has fewer levels, only the
 *       available side is written for those positions.</li>
 * </ul>
 *
 * <p>On GAP or CHECKSUM_MISMATCH the book is marked unsynced: the caller must resubscribe to get
 * a fresh snapshot. Updates received meanwhile return NOT_SYNCED.
 */
public final class OkxBookSync {

    public enum Result { SNAPSHOT, APPLIED, NOT_SYNCED, GAP, CHECKSUM_MISMATCH }

    public static final int CHECKSUM_DEPTH = 25;

    private final OrderBook book = new OrderBook();
    private boolean synced;
    private long lastSeqId = -1;
    private long gapExpectedPrevSeqId;
    private long gapReceivedPrevSeqId;
    private int mismatchExpected;
    private int mismatchComputed;

    public synchronized Result onSnapshot(long seqId, List<String[]> bids, List<String[]> asks, Integer checksum) {
        book.clear();
        book.applyAll(OrderBook.Side.BID, bids);
        book.applyAll(OrderBook.Side.ASK, asks);
        lastSeqId = seqId;
        synced = true;
        return verify(checksum, Result.SNAPSHOT);
    }

    public synchronized Result onUpdate(long seqId, long prevSeqId, List<String[]> bids, List<String[]> asks,
                                        Integer checksum) {
        if (!synced) {
            return Result.NOT_SYNCED;
        }
        if (prevSeqId != lastSeqId) {
            gapExpectedPrevSeqId = lastSeqId;
            gapReceivedPrevSeqId = prevSeqId;
            synced = false;
            return Result.GAP;
        }
        book.applyAll(OrderBook.Side.BID, bids);
        book.applyAll(OrderBook.Side.ASK, asks);
        lastSeqId = seqId;
        return verify(checksum, Result.APPLIED);
    }

    public synchronized void invalidate() {
        synced = false;
    }

    private Result verify(Integer expected, Result ok) {
        if (expected == null) {
            return ok;
        }
        int computed = checksum(book);
        if (computed != expected) {
            mismatchExpected = expected;
            mismatchComputed = computed;
            synced = false;
            return Result.CHECKSUM_MISMATCH;
        }
        return ok;
    }

    public static String checksumInput(OrderBook book) {
        List<OrderBook.Level> bids = book.topBids(CHECKSUM_DEPTH);
        List<OrderBook.Level> asks = book.topAsks(CHECKSUM_DEPTH);
        StringBuilder sb = new StringBuilder(1024);
        for (int i = 0; i < CHECKSUM_DEPTH; i++) {
            if (i < bids.size()) {
                OrderBook.Level b = bids.get(i);
                sb.append(b.rawPrice()).append(':').append(b.rawSize()).append(':');
            }
            if (i < asks.size()) {
                OrderBook.Level a = asks.get(i);
                sb.append(a.rawPrice()).append(':').append(a.rawSize()).append(':');
            }
        }
        if (!sb.isEmpty()) {
            sb.setLength(sb.length() - 1); // trailing ':'
        }
        return sb.toString();
    }

    public static int checksum(OrderBook book) {
        CRC32 crc = new CRC32();
        crc.update(checksumInput(book).getBytes(StandardCharsets.US_ASCII));
        return (int) crc.getValue();
    }

    public synchronized boolean isSynced() {
        return synced;
    }

    public synchronized boolean isCrossed() {
        return synced && book.isCrossed();
    }

    public synchronized String topOfBook() {
        return "bid=" + book.bestBid() + " ask=" + book.bestAsk();
    }

    public synchronized long lastSeqId() {
        return lastSeqId;
    }

    public synchronized String gapDetail() {
        return "expected prevSeqId=" + gapExpectedPrevSeqId + " got " + gapReceivedPrevSeqId;
    }

    public synchronized String mismatchDetail() {
        return "venue checksum=" + mismatchExpected + " local=" + mismatchComputed + " seqId=" + lastSeqId;
    }

    /** For tests and diagnostics only; callers must not mutate it. */
    synchronized OrderBook book() {
        return book;
    }
}
