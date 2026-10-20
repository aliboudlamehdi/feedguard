package io.feedguard.detect;

import io.feedguard.book.BookTop;
import io.feedguard.book.OrderBook;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReconcilerTest {

    private static BookTop top(long seq, String bidSize) {
        OrderBook b = new OrderBook();
        b.apply(OrderBook.Side.BID, "100", bidSize);
        b.apply(OrderBook.Side.ASK, "101", "1");
        return BookTop.of(b, 20, seq);
    }

    @Test
    void pairsOnEqualSequenceWhicheverSideComesFirst() {
        Reconciler r = new Reconciler(System::nanoTime, Duration.ofSeconds(10));
        assertTrue(r.begin());
        assertFalse(r.begin(), "one round at a time");
        assertNull(r.offer(Reconciler.Side.LIVE, top(10, "1")));
        assertNull(r.offer(Reconciler.Side.LIVE, top(11, "1")));
        Reconciler.Outcome o = r.offer(Reconciler.Side.REFERENCE, top(11, "1"));
        assertTrue(o.match());
        assertEquals(11, o.seq());
        assertFalse(r.isActive());
        assertEquals(1, r.matches());

        assertTrue(r.begin());
        assertNull(r.offer(Reconciler.Side.REFERENCE, top(20, "2")));
        o = r.offer(Reconciler.Side.LIVE, top(20, "1"));
        assertFalse(o.match());
        assertTrue(o.detail().contains("live=1 reference=2"), o.detail());
        assertEquals(1, r.mismatches());
    }

    @Test
    void offersOutsideARoundAreIgnored() {
        Reconciler r = new Reconciler(System::nanoTime, Duration.ofSeconds(10));
        assertNull(r.offer(Reconciler.Side.LIVE, top(1, "1")));
        assertNull(r.offer(Reconciler.Side.REFERENCE, top(1, "1")));
        assertEquals(0, r.matches());
    }

    @Test
    void timeoutAndAbortCountAsInconclusive() {
        AtomicLong now = new AtomicLong();
        Reconciler r = new Reconciler(now::get, Duration.ofSeconds(10));
        r.begin();
        r.offer(Reconciler.Side.LIVE, top(1, "1"));
        now.set(Duration.ofSeconds(5).toNanos());
        assertFalse(r.expireIfDue());
        now.set(Duration.ofSeconds(11).toNanos());
        assertTrue(r.expireIfDue());
        assertFalse(r.isActive());

        r.begin();
        r.abort();
        r.abort(); // no double count
        assertEquals(2, r.inconclusive());
        assertEquals("verified=0 mismatch=0 inconclusive=2", r.summary());
    }
}
