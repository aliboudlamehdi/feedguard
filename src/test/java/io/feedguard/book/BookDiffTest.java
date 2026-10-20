package io.feedguard.book;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BookDiffTest {

    private static OrderBook book(String... bidAskPairs) {
        OrderBook b = new OrderBook();
        for (String s : bidAskPairs) {
            String[] p = s.split(" ");
            b.apply(p[0].equals("B") ? OrderBook.Side.BID : OrderBook.Side.ASK, p[1], p[2]);
        }
        return b;
    }

    @Test
    void identicalBooksMatchEvenWithDifferentNumberFormatting() {
        BookTop a = BookTop.of(book("B 100.0 1.50", "A 101 2"), 20, 7);
        BookTop b = BookTop.of(book("B 100 1.5", "A 101.00 2.000"), 20, 7);
        assertNull(BookDiff.diff(a, b));
    }

    @Test
    void reportsSizeDifference() {
        BookTop live = BookTop.of(book("B 100 1", "A 101 2"), 20, 7);
        BookTop ref = BookTop.of(book("B 100 3", "A 101 2"), 20, 7);
        String d = BookDiff.diff(live, ref);
        assertTrue(d.startsWith("bids[0]: size at 100 live=1 reference=3"), d);
        assertTrue(d.contains("(1 differing level(s)"), d);
    }

    @Test
    void reportsMissingAndExtraLevels() {
        BookTop live = BookTop.of(book("B 100 1", "A 101 2", "A 103 1"), 20, 7);
        BookTop ref = BookTop.of(book("B 100 1", "B 99 4", "A 101 2"), 20, 7);
        String d = BookDiff.diff(live, ref);
        assertTrue(d.startsWith("bids[1]: missing in live book, reference=99x4"), d);
        assertTrue(d.contains("(2 differing level(s)"), d);
    }

    @Test
    void depthLimitsComparison() {
        OrderBook a = book("B 100 1", "B 99 1", "B 98 1");
        OrderBook b = book("B 100 1", "B 99 1", "B 98 5");
        assertNull(BookDiff.diff(BookTop.of(a, 2, 1), BookTop.of(b, 2, 1)));
        assertEquals(3, BookTop.of(a, 20, 1).bids().size());
    }
}
