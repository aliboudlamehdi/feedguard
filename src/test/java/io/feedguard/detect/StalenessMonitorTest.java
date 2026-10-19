package io.feedguard.detect;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;
import io.feedguard.testutil.RecordingSink;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StalenessMonitorTest {

    private static final long MS = 1_000_000L;

    @Test
    void emitsStaleOnceThenRecovery() {
        AtomicLong now = new AtomicLong(0);
        RecordingSink sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        StalenessMonitor mon = new StalenessMonitor(bus, now::get);
        StalenessMonitor.Handle h = mon.register(Venue.BINANCE, "BTCUSDT", Duration.ofMillis(5000));

        now.set(4_000 * MS);
        mon.check();
        assertTrue(sink.incidents.isEmpty());

        now.set(6_000 * MS);
        mon.check();
        mon.check(); // must not re-emit while still stale
        assertEquals(List.of(IncidentType.STALE_FEED), sink.types());
        assertEquals(Severity.CRITICAL, sink.last().severity());

        now.set(9_000 * MS);
        h.touch();
        mon.check();
        assertEquals(List.of(IncidentType.STALE_FEED, IncidentType.FEED_RECOVERED), sink.types());
        assertTrue(sink.last().detail().contains("9000 ms"), sink.last().detail());
    }

    @Test
    void streamsAreIndependent() {
        AtomicLong now = new AtomicLong(0);
        RecordingSink sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        StalenessMonitor mon = new StalenessMonitor(bus, now::get);
        StalenessMonitor.Handle btc = mon.register(Venue.OKX, "BTC-USDT", Duration.ofMillis(1000));
        mon.register(Venue.OKX, "ETH-USDT", Duration.ofMillis(1000));

        now.set(2_000 * MS);
        btc.touch();
        mon.check();
        assertEquals(1, sink.incidents.size());
        assertEquals("ETH-USDT", sink.last().symbol());
    }
}
