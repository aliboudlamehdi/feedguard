package io.feedguard.book;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Price-level order book. Keeps the venue's raw price/size strings because some checksums
 * (OKX, Kraken) are computed over the exact strings the venue sent.
 *
 * <p>Not thread-safe: owned by a single sync object which guards access.
 */
public final class OrderBook {

    public enum Side { BID, ASK }

    public record Level(BigDecimal price, String rawPrice, String rawSize) {
    }

    private final NavigableMap<BigDecimal, Level> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<BigDecimal, Level> asks = new TreeMap<>();

    public void clear() {
        bids.clear();
        asks.clear();
    }

    /** Sets a level; a zero size removes it. */
    public void apply(Side side, String rawPrice, String rawSize) {
        BigDecimal price = new BigDecimal(rawPrice);
        NavigableMap<BigDecimal, Level> m = side == Side.BID ? bids : asks;
        if (new BigDecimal(rawSize).signum() == 0) {
            m.remove(price);
        } else {
            m.put(price, new Level(price, rawPrice, rawSize));
        }
    }

    public void applyAll(Side side, List<String[]> levels) {
        for (String[] lvl : levels) {
            apply(side, lvl[0], lvl[1]);
        }
    }

    public BigDecimal bestBid() {
        return bids.isEmpty() ? null : bids.firstKey();
    }

    public BigDecimal bestAsk() {
        return asks.isEmpty() ? null : asks.firstKey();
    }

    public boolean isCrossed() {
        BigDecimal b = bestBid();
        BigDecimal a = bestAsk();
        return b != null && a != null && b.compareTo(a) >= 0;
    }

    public List<Level> topBids(int n) {
        return top(bids, n);
    }

    public List<Level> topAsks(int n) {
        return top(asks, n);
    }

    public int bidDepth() {
        return bids.size();
    }

    public int askDepth() {
        return asks.size();
    }

    private static List<Level> top(NavigableMap<BigDecimal, Level> side, int n) {
        if (side.isEmpty() || n <= 0) {
            return Collections.emptyList();
        }
        List<Level> out = new ArrayList<>(Math.min(n, side.size()));
        Iterator<Level> it = side.values().iterator();
        while (it.hasNext() && out.size() < n) {
            out.add(it.next());
        }
        return out;
    }
}
