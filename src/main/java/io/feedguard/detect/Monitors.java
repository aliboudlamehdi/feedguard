package io.feedguard.detect;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.RawCapture;
import io.feedguard.core.Venue;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/** Shared detection services handed to every collector. */
public final class Monitors {

    private final IncidentBus bus;
    private final StalenessMonitor staleness;
    private final SchemaDriftDetector schema;
    private final Map<String, LatencyStats> latency = new ConcurrentHashMap<>();
    private volatile RawCapture rawCapture;

    public Monitors(IncidentBus bus, LongSupplier nanoClock) {
        this.bus = bus;
        this.staleness = new StalenessMonitor(bus, nanoClock);
        this.schema = new SchemaDriftDetector(bus);
    }

    public IncidentBus bus() {
        return bus;
    }

    public StalenessMonitor staleness() {
        return staleness;
    }

    public SchemaDriftDetector schema() {
        return schema;
    }

    public LatencyStats latency(Venue venue, String symbol) {
        return latency.computeIfAbsent(venue + ":" + symbol, k -> new LatencyStats());
    }

    public void setRawCapture(RawCapture capture) {
        this.rawCapture = capture;
    }

    /** Records a raw venue message if capture is enabled. Cheap no-op otherwise. */
    public void capture(Venue venue, String message) {
        RawCapture c = rawCapture;
        if (c != null) {
            c.record(venue, message);
        }
    }

    /** Sorted view of all latency windows, keyed "VENUE:SYMBOL". */
    public Map<String, LatencyStats> allLatency() {
        return new TreeMap<>(latency);
    }
}
