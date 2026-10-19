package io.feedguard.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Appends one JSON object per incident. Incidents are rare, so each line is flushed immediately. */
public final class JsonlFileSink implements IncidentSink {

    private final BufferedWriter writer;

    public JsonlFileSink(Path path) {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            this.writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open incident file " + path, e);
        }
    }

    @Override
    public synchronized void accept(Incident incident) {
        try {
            writer.write(incident.toJson());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("Failed to write incident: " + e.getMessage());
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
