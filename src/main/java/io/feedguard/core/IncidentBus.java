package io.feedguard.core;

import java.time.Clock;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/** Fan-out of incidents to sinks, with per-type counters for the end-of-run summary. */
public final class IncidentBus {

    private final Clock clock;
    private final List<IncidentSink> sinks = new CopyOnWriteArrayList<>();
    private final Map<IncidentType, LongAdder> counts = new ConcurrentHashMap<>();

    public IncidentBus(Clock clock) {
        this.clock = clock;
    }

    public void addSink(IncidentSink sink) {
        sinks.add(sink);
    }

    public void emit(Venue venue, String symbol, IncidentType type, Severity severity, String detail) {
        Incident incident = new Incident(clock.instant(), venue, symbol, type, severity, detail);
        counts.computeIfAbsent(type, k -> new LongAdder()).increment();
        for (IncidentSink sink : sinks) {
            try {
                sink.accept(incident);
            } catch (RuntimeException e) {
                System.err.println("Incident sink failed: " + e);
            }
        }
    }

    public Map<IncidentType, Long> counts() {
        Map<IncidentType, Long> out = new EnumMap<>(IncidentType.class);
        counts.forEach((type, adder) -> out.put(type, adder.sum()));
        return out;
    }

    public void closeSinks() {
        for (IncidentSink sink : sinks) {
            sink.close();
        }
    }
}
