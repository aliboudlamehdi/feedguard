package io.feedguard.book;

import java.util.List;

/** Immutable copy of the top levels of a book, tagged with the venue sequence it reflects. */
public record BookTop(long seq, List<OrderBook.Level> bids, List<OrderBook.Level> asks) {

    public static BookTop of(OrderBook book, int depth, long seq) {
        return new BookTop(seq, List.copyOf(book.topBids(depth)), List.copyOf(book.topAsks(depth)));
    }
}
