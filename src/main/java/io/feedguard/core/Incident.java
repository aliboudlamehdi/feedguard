package io.feedguard.core;

import io.feedguard.json.Json;

import java.time.Instant;

public record Incident(Instant timestamp, Venue venue, String symbol, IncidentType type, Severity severity, String detail) {

    public String toLogLine() {
        return String.format("%s %-8s %-7s %-10s %-17s %s",
                timestamp, severity, venue, symbol, type, detail);
    }

    public String toJson() {
        return "{\"ts\":" + Json.quote(timestamp.toString())
                + ",\"venue\":" + Json.quote(venue.name())
                + ",\"symbol\":" + Json.quote(symbol)
                + ",\"type\":" + Json.quote(type.name())
                + ",\"severity\":" + Json.quote(severity.name())
                + ",\"detail\":" + Json.quote(detail)
                + "}";
    }
}
