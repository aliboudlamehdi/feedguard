package io.feedguard.detect;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Venue;
import io.feedguard.json.Json;
import io.feedguard.testutil.RecordingSink;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaDriftDetectorTest {

    private static Map<String, Object> obj(String json) {
        return Json.asObject(Json.parse(json));
    }

    @Test
    void reportsAddedAndRemovedFieldsOncePerChange() {
        RecordingSink sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        SchemaDriftDetector d = new SchemaDriftDetector(bus);

        d.observe(Venue.BINANCE, "BTCUSDT", "depthUpdate", obj("{\"e\":1,\"E\":2,\"U\":3}"));
        d.observe(Venue.BINANCE, "BTCUSDT", "depthUpdate", obj("{\"U\":3,\"E\":2,\"e\":1}")); // order irrelevant
        assertTrue(sink.incidents.isEmpty());

        d.observe(Venue.BINANCE, "BTCUSDT", "depthUpdate", obj("{\"e\":1,\"E\":2,\"pu\":9}"));
        d.observe(Venue.BINANCE, "ETHUSDT", "depthUpdate", obj("{\"e\":1,\"E\":2,\"pu\":9}")); // same change
        assertEquals(List.of(IncidentType.SCHEMA_DRIFT), sink.types());
        assertTrue(sink.last().detail().contains("added=[pu]"), sink.last().detail());
        assertTrue(sink.last().detail().contains("removed=[U]"), sink.last().detail());
    }

    @Test
    void channelsHaveSeparateBaselines() {
        RecordingSink sink = new RecordingSink();
        IncidentBus bus = new IncidentBus(Clock.systemUTC());
        bus.addSink(sink);
        SchemaDriftDetector d = new SchemaDriftDetector(bus);
        d.observe(Venue.OKX, "BTC-USDT", "books/snapshot", obj("{\"a\":1}"));
        d.observe(Venue.OKX, "BTC-USDT", "books/update", obj("{\"b\":1}"));
        assertTrue(sink.incidents.isEmpty());
    }
}
