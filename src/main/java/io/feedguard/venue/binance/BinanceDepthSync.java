package io.feedguard.venue.binance;

import io.feedguard.book.BookTop;
import io.feedguard.book.OrderBook;

import java.util.ArrayDeque;
import java.util.List;

/**
 * Binance spot local order book synchronisation (diff-depth stream + REST snapshot), following
 * Binance's documented procedure, instrumented to tell apart three situations:
 *
 * <ul>
 *   <li>{@link Result#SNAPSHOT_TOO_OLD}: the REST snapshot predates the first buffered event.
 *       Normal during startup; refetch, not a feed incident.</li>
 *   <li>{@link Result#GAP}: on a live book, an event's first update id U is not the previous
 *       event's final update id u + 1. Updates were lost (or duplicated/out of order): a real
 *       feed incident.</li>
 *   <li>{@link Result#STALE_DROPPED}: event entirely covered by the snapshot, ignored.</li>
 * </ul>
 *
 * <p>After GAP or SNAPSHOT_TOO_OLD the offending event is kept at the head of the buffer and
 * events are buffered until {@link #onSnapshot} is called again.
 *
 * <p>Thread-safe: WebSocket and REST callbacks may arrive on different threads.
 */
public final class BinanceDepthSync {

    public record DepthEvent(long firstUpdateId, long finalUpdateId, long eventTimeMs,
                             List<String[]> bids, List<String[]> asks) {
    }

    /** {@code missing() < 0} means overlap / out-of-order rather than loss. */
    public record Gap(long expectedFirstUpdateId, long receivedFirstUpdateId) {
        public long missing() {
            return receivedFirstUpdateId - expectedFirstUpdateId;
        }
    }

    public enum Result { BUFFERED, STALE_DROPPED, APPLIED, GAP, SNAPSHOT_TOO_OLD }

    static final int MAX_BUFFER = 50_000;

    private final OrderBook book = new OrderBook();
    private final ArrayDeque<DepthEvent> buffer = new ArrayDeque<>();
    private boolean snapshotLoaded;
    private boolean firstEventApplied;
    private long lastUpdateId = -1;
    private Gap lastGap;
    private long bufferOverflows;

    public synchronized Result onEvent(DepthEvent ev) {
        if (!snapshotLoaded) {
            if (buffer.size() >= MAX_BUFFER) {
                buffer.pollFirst();
                bufferOverflows++;
            }
            buffer.addLast(ev);
            return Result.BUFFERED;
        }
        return process(ev);
    }

    /** Loads a REST snapshot then replays buffered events. Returns APPLIED or SNAPSHOT_TOO_OLD / GAP. */
    public synchronized Result onSnapshot(long snapshotLastUpdateId, List<String[]> bids, List<String[]> asks) {
        book.clear();
        book.applyAll(OrderBook.Side.BID, bids);
        book.applyAll(OrderBook.Side.ASK, asks);
        lastUpdateId = snapshotLastUpdateId;
        snapshotLoaded = true;
        firstEventApplied = false;
        while (!buffer.isEmpty()) {
            Result r = process(buffer.pollFirst());
            if (r == Result.GAP || r == Result.SNAPSHOT_TOO_OLD) {
                return r;
            }
        }
        return Result.APPLIED;
    }

    /** Forget everything (e.g. after a reconnect). Events are buffered until the next snapshot. */
    public synchronized void reset() {
        buffer.clear();
        book.clear();
        snapshotLoaded = false;
        firstEventApplied = false;
        lastUpdateId = -1;
    }

    private Result process(DepthEvent ev) {
        if (ev.finalUpdateId() <= lastUpdateId) {
            return Result.STALE_DROPPED;
        }
        long expected = lastUpdateId + 1;
        if (!firstEventApplied) {
            // Binance rule: first event must satisfy U <= lastUpdateId + 1 <= u.
            // u >= expected already holds (u > lastUpdateId above), so only U needs checking.
            if (ev.firstUpdateId() > expected) {
                lastGap = new Gap(expected, ev.firstUpdateId());
                invalidateKeeping(ev);
                return Result.SNAPSHOT_TOO_OLD;
            }
            apply(ev);
            firstEventApplied = true;
            return Result.APPLIED;
        }
        if (ev.firstUpdateId() != expected) {
            lastGap = new Gap(expected, ev.firstUpdateId());
            invalidateKeeping(ev);
            return Result.GAP;
        }
        apply(ev);
        return Result.APPLIED;
    }

    private void apply(DepthEvent ev) {
        book.applyAll(OrderBook.Side.BID, ev.bids());
        book.applyAll(OrderBook.Side.ASK, ev.asks());
        lastUpdateId = ev.finalUpdateId();
    }

    private void invalidateKeeping(DepthEvent ev) {
        snapshotLoaded = false;
        firstEventApplied = false;
        buffer.addFirst(ev);
    }

    public synchronized boolean isLive() {
        return snapshotLoaded && firstEventApplied;
    }

    public synchronized boolean isCrossed() {
        return isLive() && book.isCrossed();
    }

    /** Copy of the top levels tagged with the last applied update id. */
    public synchronized BookTop top(int depth) {
        return BookTop.of(book, depth, lastUpdateId);
    }

    public synchronized String topOfBook() {
        return "bid=" + book.bestBid() + " ask=" + book.bestAsk();
    }

    public synchronized long lastUpdateId() {
        return lastUpdateId;
    }

    public synchronized Gap lastGap() {
        return lastGap;
    }

    public synchronized int bufferedEvents() {
        return buffer.size();
    }

    public synchronized long bufferOverflows() {
        return bufferOverflows;
    }

    /** For tests and diagnostics only; callers must not mutate it. */
    synchronized OrderBook book() {
        return book;
    }
}
