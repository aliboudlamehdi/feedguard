package io.feedguard.book;

import java.math.BigDecimal;
import java.util.List;

/** Compares two book tops level by level (prices and sizes compared numerically). */
public final class BookDiff {

    private BookDiff() {
    }

    /** Returns null when identical, otherwise a description of the first difference and the total count. */
    public static String diff(BookTop live, BookTop reference) {
        StringBuilder first = new StringBuilder();
        int count = compareSide("bids", live.bids(), reference.bids(), first)
                + compareSide("asks", live.asks(), reference.asks(), first);
        if (count == 0) {
            return null;
        }
        return first + " (" + count + " differing level(s) in top " + Math.max(
                Math.max(live.bids().size(), reference.bids().size()),
                Math.max(live.asks().size(), reference.asks().size())) + ")";
    }

    private static int compareSide(String side, List<OrderBook.Level> live, List<OrderBook.Level> ref,
                                   StringBuilder first) {
        int count = 0;
        int n = Math.max(live.size(), ref.size());
        for (int i = 0; i < n; i++) {
            OrderBook.Level a = i < live.size() ? live.get(i) : null;
            OrderBook.Level b = i < ref.size() ? ref.get(i) : null;
            String d = levelDiff(a, b);
            if (d != null) {
                if (count == 0 && first.isEmpty()) {
                    first.append(side).append('[').append(i).append("]: ").append(d);
                }
                count++;
            }
        }
        return count;
    }

    private static String levelDiff(OrderBook.Level live, OrderBook.Level ref) {
        if (live == null) {
            return "missing in live book, reference=" + fmt(ref);
        }
        if (ref == null) {
            return "extra in live book " + fmt(live);
        }
        if (live.price().compareTo(ref.price()) != 0) {
            return "price live=" + fmt(live) + " reference=" + fmt(ref);
        }
        if (new BigDecimal(live.rawSize()).compareTo(new BigDecimal(ref.rawSize())) != 0) {
            return "size at " + live.rawPrice() + " live=" + live.rawSize() + " reference=" + ref.rawSize();
        }
        return null;
    }

    private static String fmt(OrderBook.Level l) {
        return l.rawPrice() + "x" + l.rawSize();
    }
}
