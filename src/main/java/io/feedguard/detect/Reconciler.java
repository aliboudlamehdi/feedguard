package io.feedguard.detect;

import io.feedguard.book.BookDiff;
import io.feedguard.book.BookTop;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * One reconciliation round at a time for one book: the live book and an independently built
 * reference book offer their tops tagged with a venue sequence; when both sides have offered the
 * same sequence, the tops are compared and the round ends.
 *
 * <p>Thread-safe: live and reference sides may run on different threads.
 */
public final class Reconciler {

    public enum Side { LIVE, REFERENCE }

    public record Outcome(boolean match, long seq, String detail) {
    }

    private static final int MAX_PENDING = 512;

    private final LongSupplier clock;
    private final long timeoutNanos;
    private final Map<Long, BookTop> live = new LinkedHashMap<>();
    private final Map<Long, BookTop> reference = new LinkedHashMap<>();
    private boolean active;
    private long startedAt;
    private long matches;
    private long mismatches;
    private long inconclusive;

    public Reconciler(LongSupplier clock, Duration timeout) {
        this.clock = clock;
        this.timeoutNanos = timeout.toNanos();
    }

    /** Starts a round; returns false if one is already running. */
    public synchronized boolean begin() {
        if (active) {
            return false;
        }
        active = true;
        startedAt = clock.getAsLong();
        live.clear();
        reference.clear();
        return true;
    }

    public synchronized boolean isActive() {
        return active;
    }

    /** Returns the outcome when this offer completes a pair, otherwise null. */
    public synchronized Outcome offer(Side side, BookTop top) {
        if (!active) {
            return null;
        }
        Map<Long, BookTop> mine = side == Side.LIVE ? live : reference;
        Map<Long, BookTop> other = side == Side.LIVE ? reference : live;
        BookTop counterpart = other.get(top.seq());
        if (counterpart == null) {
            mine.put(top.seq(), top);
            trim(mine);
            return null;
        }
        String diff = side == Side.LIVE ? BookDiff.diff(top, counterpart) : BookDiff.diff(counterpart, top);
        finish();
        if (diff == null) {
            matches++;
            return new Outcome(true, top.seq(), null);
        }
        mismatches++;
        return new Outcome(false, top.seq(), diff);
    }

    /** Ends the round as inconclusive if it has run past its timeout. Returns true if it did. */
    public synchronized boolean expireIfDue() {
        if (active && clock.getAsLong() - startedAt > timeoutNanos) {
            finish();
            inconclusive++;
            return true;
        }
        return false;
    }

    /** Ends the round as inconclusive (reference could not be built, feed incident meanwhile...). */
    public synchronized void abort() {
        if (active) {
            finish();
            inconclusive++;
        }
    }

    public synchronized String summary() {
        return "verified=" + matches + " mismatch=" + mismatches + " inconclusive=" + inconclusive;
    }

    public synchronized long matches() {
        return matches;
    }

    public synchronized long mismatches() {
        return mismatches;
    }

    public synchronized long inconclusive() {
        return inconclusive;
    }

    private void finish() {
        active = false;
        live.clear();
        reference.clear();
    }

    private static void trim(Map<Long, BookTop> m) {
        Iterator<Long> it = m.keySet().iterator();
        while (m.size() > MAX_PENDING && it.hasNext()) {
            it.next();
            it.remove();
        }
    }
}
