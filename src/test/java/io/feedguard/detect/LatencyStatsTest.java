package io.feedguard.detect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LatencyStatsTest {

    @Test
    void percentilesAndReset() {
        LatencyStats s = new LatencyStats();
        for (int i = 1; i <= 100; i++) {
            s.record(i);
        }
        s.record(-3);
        LatencyStats.Snapshot snap = s.snapshotAndReset();
        assertEquals(101, snap.count());
        assertEquals(50, snap.p50());
        assertEquals(99, snap.p99());
        assertEquals(100, snap.max());
        assertEquals(1, snap.negativeCount());
        assertEquals(LatencyStats.Snapshot.EMPTY, s.snapshotAndReset());
    }

    @Test
    void growsBeyondInitialCapacity() {
        LatencyStats s = new LatencyStats();
        for (int i = 0; i < 10_000; i++) {
            s.record(7);
        }
        LatencyStats.Snapshot snap = s.snapshotAndReset();
        assertEquals(10_000, snap.count());
        assertEquals(7, snap.p99());
    }
}
