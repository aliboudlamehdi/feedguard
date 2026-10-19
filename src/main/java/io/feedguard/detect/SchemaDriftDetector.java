package io.feedguard.detect;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Flags messages whose field set differs from the first message seen on the same channel —
 * the early warning before a venue API change breaks a parser in production.
 * Each distinct change is reported once.
 */
public final class SchemaDriftDetector {

    private final IncidentBus bus;
    private final Map<String, Set<String>> baselines = new ConcurrentHashMap<>();
    private final Set<String> reported = ConcurrentHashMap.newKeySet();

    public SchemaDriftDetector(IncidentBus bus) {
        this.bus = bus;
    }

    public void observe(Venue venue, String symbol, String channel, Map<String, Object> message) {
        if (message == null) {
            return;
        }
        String key = venue + "|" + channel;
        Set<String> base = baselines.get(key);
        if (base == null) {
            baselines.putIfAbsent(key, Set.copyOf(message.keySet()));
            return;
        }
        if (base.size() == message.size() && base.containsAll(message.keySet())) {
            return; // fast path, no allocation
        }
        Set<String> added = new TreeSet<>(message.keySet());
        added.removeAll(base);
        Set<String> removed = new TreeSet<>(base);
        removed.removeAll(message.keySet());
        String signature = key + "|+" + added + "|-" + removed;
        if (reported.add(signature)) {
            bus.emit(venue, symbol, IncidentType.SCHEMA_DRIFT, Severity.WARN,
                    "channel=" + channel + " added=" + added + " removed=" + removed);
        }
    }
}
