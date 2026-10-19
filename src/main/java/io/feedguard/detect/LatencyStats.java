package io.feedguard.detect;

import java.util.Arrays;

/**
 * Windowed distribution of (local receive time - venue event time), in milliseconds.
 *
 * <p>The value mixes network latency, venue publishing delay and clock offset, so run the probe
 * with chrony/PTP-synchronised time. Negative values are counted separately: they reveal clock
 * skew rather than latency.
 */
public final class LatencyStats {

    public record Snapshot(int count, long p50, long p99, long max, long negativeCount) {
        public static final Snapshot EMPTY = new Snapshot(0, 0, 0, 0, 0);
    }

    private static final int INITIAL = 4096;
    private static final int SHRINK_ABOVE = 1 << 16;

    private long[] samples = new long[INITIAL];
    private int n;
    private long negative;

    public synchronized void record(long lagMs) {
        if (lagMs < 0) {
            negative++;
        }
        if (n == samples.length) {
            samples = Arrays.copyOf(samples, n * 2);
        }
        samples[n++] = lagMs;
    }

    public synchronized Snapshot snapshotAndReset() {
        if (n == 0) {
            return Snapshot.EMPTY;
        }
        long[] sorted = Arrays.copyOf(samples, n);
        Arrays.sort(sorted);
        Snapshot s = new Snapshot(n, percentile(sorted, 0.50), percentile(sorted, 0.99), sorted[n - 1], negative);
        n = 0;
        negative = 0;
        if (samples.length > SHRINK_ABOVE) {
            samples = new long[INITIAL];
        }
        return s;
    }

    private static long percentile(long[] sorted, double q) {
        int idx = (int) Math.ceil(q * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(idx, sorted.length - 1))];
    }
}
