package io.feedguard.core;

public interface IncidentSink extends AutoCloseable {

    void accept(Incident incident);

    @Override
    default void close() {
    }
}
