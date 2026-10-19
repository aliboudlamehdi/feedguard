package io.feedguard.book;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderBookTest {

    @Test
    void bestPricesAndOrdering() {
        OrderBook b = new OrderBook();
        b.apply(OrderBook.Side.BID, "100.0", "1");
        b.apply(OrderBook.Side.BID, "101.5", "2");
        b.apply(OrderBook.Side.ASK, "103", "1");
        b.apply(OrderBook.Side.ASK, "102", "4");
        assertEquals(new BigDecimal("101.5"), b.bestBid());
        assertEquals(new BigDecimal("102"), b.bestAsk());
        assertEquals("101.5", b.topBids(1).get(0).rawPrice());
        assertEquals("102", b.topAsks(2).get(0).rawPrice());
        assertEquals("103", b.topAsks(2).get(1).rawPrice());
    }

    @Test
    void zeroSizeRemovesLevelWhateverTheFormatting() {
        OrderBook b = new OrderBook();
        b.apply(OrderBook.Side.BID, "100.10", "5");
        b.apply(OrderBook.Side.BID, "100.1", "0.00000000"); // same price, different string
        assertNull(b.bestBid());
        assertEquals(0, b.bidDepth());
    }

    @Test
    void updateReplacesSizeAndKeepsLatestRawStrings() {
        OrderBook b = new OrderBook();
        b.apply(OrderBook.Side.ASK, "50", "1");
        b.apply(OrderBook.Side.ASK, "50", "2.5");
        assertEquals(1, b.askDepth());
        assertEquals("2.5", b.topAsks(1).get(0).rawSize());
    }

    @Test
    void crossedDetection() {
        OrderBook b = new OrderBook();
        assertFalse(b.isCrossed());
        b.apply(OrderBook.Side.BID, "99", "1");
        b.apply(OrderBook.Side.ASK, "100", "1");
        assertFalse(b.isCrossed());
        b.apply(OrderBook.Side.BID, "100", "1"); // locked = crossed for our purposes
        assertTrue(b.isCrossed());
    }
}
