package io.feedguard.ws;

import io.feedguard.core.IncidentBus;
import io.feedguard.core.IncidentType;
import io.feedguard.core.Severity;
import io.feedguard.core.Venue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * JDK WebSocket client with exponential-backoff reconnect, serialized sends, optional text
 * heartbeat, and DISCONNECT / RECONNECTED incidents. Protocol-level pings from the venue are
 * answered automatically by the JDK implementation.
 */
public final class ResilientWebSocket implements WebSocket.Listener, AutoCloseable {

    private static final long MAX_BACKOFF_MS = 30_000;

    private final Venue venue;
    private final URI uri;
    private final HttpClient http;
    private final ScheduledExecutorService scheduler;
    private final IncidentBus bus;
    private final Consumer<String> onMessage;
    private final Consumer<Consumer<String>> onConnected;
    private final String heartbeatText;
    private final Duration heartbeatInterval;

    private final StringBuilder partial = new StringBuilder();
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean();
    private volatile WebSocket ws;
    private volatile boolean closed;
    private CompletableFuture<?> sendChain = CompletableFuture.completedFuture(null);
    private ScheduledFuture<?> heartbeat;
    private int attempts;
    private long disconnectedAtNanos;

    /**
     * @param onMessage   called with each complete text message (listener thread, sequential)
     * @param onConnected called on every (re)connection with a sender, e.g. to (re)subscribe
     * @param heartbeatText text frame sent periodically, or null (OKX expects "ping")
     */
    public ResilientWebSocket(Venue venue, URI uri, HttpClient http, ScheduledExecutorService scheduler,
                              IncidentBus bus, Consumer<String> onMessage, Consumer<Consumer<String>> onConnected,
                              String heartbeatText, Duration heartbeatInterval) {
        this.venue = venue;
        this.uri = uri;
        this.http = http;
        this.scheduler = scheduler;
        this.bus = bus;
        this.onMessage = onMessage;
        this.onConnected = onConnected;
        this.heartbeatText = heartbeatText;
        this.heartbeatInterval = heartbeatInterval;
    }

    public void start() {
        connect();
    }

    private void connect() {
        if (closed) {
            return;
        }
        http.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(uri, this)
                .whenComplete((w, err) -> {
                    if (err != null) {
                        bus.emit(venue, "*", IncidentType.DISCONNECT, Severity.WARN,
                                "connect failed: " + rootMessage(err));
                        scheduleReconnect();
                    }
                });
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        this.ws = webSocket;
        synchronized (this) {
            sendChain = CompletableFuture.completedFuture(null);
        }
        if (disconnectedAtNanos != 0) {
            bus.emit(venue, "*", IncidentType.RECONNECTED, Severity.INFO,
                    "after " + (System.nanoTime() - disconnectedAtNanos) / 1_000_000 + " ms, "
                            + attempts + " attempt(s)");
            disconnectedAtNanos = 0;
        }
        attempts = 0;
        startHeartbeat();
        try {
            onConnected.accept(this::send);
        } finally {
            webSocket.request(1);
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            String message = partial.toString();
            partial.setLength(0);
            try {
                onMessage.accept(message);
            } catch (RuntimeException e) {
                bus.emit(venue, "*", IncidentType.PARSE_ERROR, Severity.WARN,
                        e.getClass().getSimpleName() + ": " + e.getMessage() + " | msg=" + truncate(message));
            }
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        handleDisconnect("closed by venue code=" + statusCode + " reason=" + reason);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        handleDisconnect("socket error: " + rootMessage(error));
    }

    /** Thread-safe; sends are chained because the JDK WebSocket forbids overlapping sends. */
    public synchronized void send(String text) {
        WebSocket w = ws;
        if (w == null || closed) {
            return;
        }
        sendChain = sendChain
                .exceptionally(t -> null)
                .thenCompose(ignored -> w.sendText(text, true));
    }

    private void handleDisconnect(String why) {
        stopHeartbeat();
        ws = null;
        partial.setLength(0);
        if (closed) {
            return;
        }
        if (disconnectedAtNanos == 0) {
            disconnectedAtNanos = System.nanoTime();
        }
        bus.emit(venue, "*", IncidentType.DISCONNECT, Severity.CRITICAL, why);
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        if (closed || !reconnectScheduled.compareAndSet(false, true)) {
            return;
        }
        if (disconnectedAtNanos == 0) {
            disconnectedAtNanos = System.nanoTime();
        }
        long delay = Math.min(MAX_BACKOFF_MS, 500L << Math.min(attempts, 6));
        attempts++;
        scheduler.schedule(() -> {
            reconnectScheduled.set(false);
            connect();
        }, delay, TimeUnit.MILLISECONDS);
    }

    private synchronized void startHeartbeat() {
        stopHeartbeat();
        if (heartbeatText != null && heartbeatInterval != null) {
            long ms = heartbeatInterval.toMillis();
            heartbeat = scheduler.scheduleAtFixedRate(() -> send(heartbeatText), ms, ms, TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void stopHeartbeat() {
        if (heartbeat != null) {
            heartbeat.cancel(false);
            heartbeat = null;
        }
    }

    @Override
    public void close() {
        closed = true;
        stopHeartbeat();
        WebSocket w = ws;
        if (w != null) {
            w.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
        }
    }

    static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null && c.getCause() != c) {
            c = c.getCause();
        }
        return c.getClass().getSimpleName() + (c.getMessage() == null ? "" : ": " + c.getMessage());
    }

    private static String truncate(String s) {
        return s.length() <= 200 ? s : s.substring(0, 200) + "...";
    }
}
