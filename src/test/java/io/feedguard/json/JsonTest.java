package io.feedguard.json;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonTest {

    @Test
    void parsesBinanceDepthMessage() {
        String msg = """
                {"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1700000000123,"s":"BTCUSDT",
                "U":157,"u":160,"b":[["0.0024","10"]],"a":[["0.0026","100"],["0.0027","0.00000000"]]}}""";
        Map<String, Object> root = Json.asObject(Json.parse(msg));
        Map<String, Object> data = Json.obj(root, "data");
        assertEquals("BTCUSDT", Json.str(data, "s"));
        assertEquals(1700000000123L, Json.lng(data, "E", -1));
        assertEquals(157L, Json.lng(data, "U", -1));
        List<String[]> asks = Json.levels(Json.arr(data, "a"));
        assertEquals(2, asks.size());
        assertEquals("0.0027", asks.get(1)[0]);
        assertEquals("0.00000000", asks.get(1)[1]);
    }

    @Test
    void parsesNegativeChecksumAndNumericStrings() {
        Map<String, Object> d = Json.asObject(Json.parse(
                "{\"checksum\":-1200119424,\"ts\":\"1597026383085\",\"seqId\":123456,\"x\":1.5e2,\"n\":null,\"b\":true}"));
        assertEquals(-1200119424, (int) Json.lng(d, "checksum", 0));
        assertEquals(1597026383085L, Json.lng(d, "ts", -1));
        assertEquals(new BigDecimal("1.5e2"), d.get("x"));
        assertNull(d.get("n"));
        assertEquals(Boolean.TRUE, d.get("b"));
        assertTrue(Json.has(d, "n"));
    }

    @Test
    void handlesEscapesAndUnicode() {
        Object v = Json.parse("\"a\\\"b\\\\c\\n\\u00e9\"");
        assertEquals("a\"b\\c\né", v);
        assertEquals("\"a\\\"b\\\\c\\né\"", Json.quote("a\"b\\c\né"));
    }

    @Test
    void keepsFieldOrder() {
        Map<String, Object> m = Json.asObject(Json.parse("{\"z\":1,\"a\":2,\"m\":3}"));
        assertEquals(List.of("z", "a", "m"), List.copyOf(m.keySet()));
    }

    @Test
    void rejectsMalformedInput() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":1}x"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[1,]"));
        assertThrows(IllegalArgumentException.class, () -> Json.levels(List.of(List.of("1"))));
    }

    @Test
    void lngFallsBackOnNonNumeric() {
        Map<String, Object> d = Json.asObject(Json.parse("{\"a\":\"abc\",\"b\":\"\"}"));
        assertEquals(-1, Json.lng(d, "a", -1));
        assertEquals(-1, Json.lng(d, "b", -1));
        assertEquals(-1, Json.lng(d, "missing", -1));
    }
}
