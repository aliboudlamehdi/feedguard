package io.feedguard.json;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal, dependency-free JSON parser for exchange messages.
 *
 * <p>Objects become {@code LinkedHashMap<String,Object>} (field order kept, useful for schema
 * drift), arrays become {@code ArrayList<Object>}, integers become {@code Long}, other numbers
 * {@code BigDecimal}, plus {@code String}, {@code Boolean} and {@code null}.
 *
 * <p>Prototype trade-off: this allocates a tree per message. A zero-allocation, field-skipping
 * parser is the planned replacement once the detectors are validated on live data.
 */
public final class Json {

    private final CharSequence s;
    private int pos;

    private Json(CharSequence s) {
        this.s = s;
    }

    public static Object parse(CharSequence text) {
        Json p = new Json(text);
        p.skipWs();
        Object v = p.readValue();
        p.skipWs();
        if (p.pos != p.s.length()) {
            throw p.error("trailing characters");
        }
        return v;
    }

    // ---------------------------------------------------------------- accessors

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asObject(Object o) {
        return o instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
    }

    @SuppressWarnings("unchecked")
    public static List<Object> asArray(Object o) {
        return o instanceof List<?> l ? (List<Object>) l : null;
    }

    public static Map<String, Object> obj(Map<String, Object> m, String key) {
        return m == null ? null : asObject(m.get(key));
    }

    public static List<Object> arr(Map<String, Object> m, String key) {
        return m == null ? null : asArray(m.get(key));
    }

    public static String str(Map<String, Object> m, String key) {
        Object v = m == null ? null : m.get(key);
        return v == null ? null : v.toString();
    }

    /** Reads a long whether the venue sent it as a JSON number or a numeric string. */
    public static long lng(Map<String, Object> m, String key, long dflt) {
        Object v = m == null ? null : m.get(key);
        if (v instanceof Long l) {
            return l;
        }
        if (v instanceof BigDecimal bd) {
            return bd.longValue();
        }
        if (v instanceof String str && !str.isEmpty()) {
            try {
                return Long.parseLong(str);
            } catch (NumberFormatException e) {
                return dflt;
            }
        }
        return dflt;
    }

    public static boolean has(Map<String, Object> m, String key) {
        return m != null && m.containsKey(key);
    }

    /** Converts {@code [["price","size",...], ...]} into a list of {@code [price, size]} raw strings. */
    public static List<String[]> levels(List<Object> raw) {
        if (raw == null) {
            return List.of();
        }
        List<String[]> out = new ArrayList<>(raw.size());
        for (Object o : raw) {
            List<Object> lvl = asArray(o);
            if (lvl == null || lvl.size() < 2) {
                throw new IllegalArgumentException("Malformed price level: " + o);
            }
            out.add(new String[]{lvl.get(0).toString(), lvl.get(1).toString()});
        }
        return out;
    }

    public static String quote(String v) {
        if (v == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(v.length() + 2).append('"');
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    // ---------------------------------------------------------------- parser

    private Object readValue() {
        if (pos >= s.length()) {
            throw error("unexpected end of input");
        }
        char c = s.charAt(pos);
        return switch (c) {
            case '{' -> readObject();
            case '[' -> readArray();
            case '"' -> readString();
            case 't' -> literal("true", Boolean.TRUE);
            case 'f' -> literal("false", Boolean.FALSE);
            case 'n' -> literal("null", null);
            default -> {
                if (c == '-' || (c >= '0' && c <= '9')) {
                    yield readNumber();
                }
                throw error("unexpected character '" + c + "'");
            }
        };
    }

    private Map<String, Object> readObject() {
        pos++; // {
        Map<String, Object> m = new LinkedHashMap<>();
        skipWs();
        if (peek() == '}') {
            pos++;
            return m;
        }
        while (true) {
            skipWs();
            if (peek() != '"') {
                throw error("expected field name");
            }
            String key = readString();
            skipWs();
            expect(':');
            skipWs();
            m.put(key, readValue());
            skipWs();
            char c = next();
            if (c == '}') {
                return m;
            }
            if (c != ',') {
                throw error("expected ',' or '}'");
            }
        }
    }

    private List<Object> readArray() {
        pos++; // [
        List<Object> l = new ArrayList<>();
        skipWs();
        if (peek() == ']') {
            pos++;
            return l;
        }
        while (true) {
            skipWs();
            l.add(readValue());
            skipWs();
            char c = next();
            if (c == ']') {
                return l;
            }
            if (c != ',') {
                throw error("expected ',' or ']'");
            }
        }
    }

    private String readString() {
        pos++; // opening quote
        StringBuilder sb = null;
        int start = pos;
        while (true) {
            if (pos >= s.length()) {
                throw error("unterminated string");
            }
            char c = s.charAt(pos);
            if (c == '"') {
                String out = sb == null ? s.subSequence(start, pos).toString()
                        : sb.append(s, start, pos).toString();
                pos++;
                return out;
            }
            if (c == '\\') {
                if (sb == null) {
                    sb = new StringBuilder();
                }
                sb.append(s, start, pos);
                pos++;
                char e = next();
                switch (e) {
                    case '"', '\\', '/' -> sb.append(e);
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        if (pos + 4 > s.length()) {
                            throw error("bad unicode escape");
                        }
                        sb.append((char) Integer.parseInt(s.subSequence(pos, pos + 4).toString(), 16));
                        pos += 4;
                    }
                    default -> throw error("bad escape '\\" + e + "'");
                }
                start = pos;
            } else {
                pos++;
            }
        }
    }

    private Object readNumber() {
        int start = pos;
        boolean integral = true;
        if (peek() == '-') {
            pos++;
        }
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c >= '0' && c <= '9') {
                pos++;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                integral = false;
                pos++;
            } else {
                break;
            }
        }
        String raw = s.subSequence(start, pos).toString();
        if (integral && raw.length() < 19) {
            return Long.parseLong(raw);
        }
        BigDecimal bd = new BigDecimal(raw);
        if (integral) {
            try {
                return bd.longValueExact();
            } catch (ArithmeticException ignored) {
                return bd;
            }
        }
        return bd;
    }

    private Object literal(String word, Object value) {
        if (pos + word.length() > s.length() || !s.subSequence(pos, pos + word.length()).toString().equals(word)) {
            throw error("bad literal");
        }
        pos += word.length();
        return value;
    }

    private void skipWs() {
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c == ' ' || c == '\n' || c == '\r' || c == '\t') {
                pos++;
            } else {
                return;
            }
        }
    }

    private char peek() {
        if (pos >= s.length()) {
            throw error("unexpected end of input");
        }
        return s.charAt(pos);
    }

    private char next() {
        char c = peek();
        pos++;
        return c;
    }

    private void expect(char c) {
        if (next() != c) {
            throw error("expected '" + c + "'");
        }
    }

    private IllegalArgumentException error(String msg) {
        return new IllegalArgumentException("JSON parse error at " + pos + ": " + msg);
    }
}
