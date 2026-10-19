package io.feedguard.detect;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;

/**
 * Detects streams that go silent while the socket stays "up" — the failure mode that
 * heartbeats and TCP keep-alives do not catch.
 *
 * <p>Collectors call {@link Handle#touch()} on every message (one volatile write, no allocation);
 * a scheduler calls {@link #check()} periodically.
 */
public final class StalenessMonitor {

    public static final class Handle {
        private final Venue venue;
        private final String symbol;
        private final long thresholdNanos;
        private final LongSupplier clock;
        private volatile long lastNanos;
        // Only touched by the checking thread.
        private boolean stale;
        private long staleSinceNanos;

        private Handle(Venue venue, String symbol, long thresholdNanos, LongSupplier clock) {
            this.venue = venue;
            this.symbol = symbol;
            this.thresholdNanos = thresholdNanos;
            this.clock = clock;
            this.lastNanos = clock.getAsLong();
        }

        public void touch() {
            lastNanos = clock.getAsLong();
        }
    }

    private final IncidentBus bus;
    private final LongSupplier nanoClock;
    private final List<Handle> handles = new CopyOnWriteArrayList<>();

    public StalenessMonitor(IncidentBus bus, LongSupplier nanoClock) {
        this.bus = bus;
        this.nanoClock = nanoClock;
    }

    public Handle register(Venue venue, String symbol, Duration threshold) {
        Handle h = new Handle(venue, symbol, threshold.toNanos(), nanoClock);
        handles.add(h);
        return h;
    }

    public void check() {
        long now = nanoClock.getAsLong();
        for (Handle h : handles) {
            long last = h.lastNanos;
            long age = now - last;
            if (!h.stale && age > h.thresholdNanos) {
                h.stale = true;
                h.staleSinceNanos = last;
                bus.emit(h.venue, h.symbol, IncidentType.STALE_FEED, Severity.CRITICAL,
                        "no message for " + age / 1_000_000 + " ms (threshold "
                                + h.thresholdNanos / 1_000_000 + " ms)");
            } else if (h.stale && age <= h.thresholdNanos) {
                h.stale = false;
                bus.emit(h.venue, h.symbol, IncidentType.FEED_RECOVERED, Severity.INFO,
                        "stream silent for " + (last - h.staleSinceNanos) / 1_000_000 + " ms");
            }
        }
    }
}
