package io.feedguard.core;

public final class ConsoleSink implements IncidentSink {

    @Override
    public synchronized void accept(Incident incident) {
        System.out.println(incident.toLogLine());
    }
}
