package io.feedguard.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

/**
 * Writes the first N raw messages received from each venue, one per line prefixed with
 * receive time and venue. Used to check venue message formats against our parsers
 * (e.g. confirming what OKX actually sends in the "checksum" field).
 */
public final class RawCapture implements AutoCloseable {

    private final BufferedWriter writer;
    private final int limitPerVenue;
    private final Map<Venue, Integer> counts = new EnumMap<>(Venue.class);

    public RawCapture(Path path, int limitPerVenue) {
        this.limitPerVenue = limitPerVenue;
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open raw capture file " + path, e);
        }
    }

    public synchronized void record(Venue venue, String message) {
        int n = counts.getOrDefault(venue, 0);
        if (n >= limitPerVenue) {
            return;
        }
        counts.put(venue, n + 1);
        try {
            writer.write(Instant.now() + "\t" + venue + "\t" + message);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("Raw capture write failed: " + e.getMessage());
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.close();
        } catch (IOException ignored) {
            // shutting down
        }
    }
}
