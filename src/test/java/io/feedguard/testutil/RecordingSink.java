package io.feedguard.testutil;

import io.feedguard.core.Incident;
import io.feedguard.core.IncidentSink;
import io.feedguard.core.IncidentType;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class RecordingSink implements IncidentSink {

    public final List<Incident> incidents = new CopyOnWriteArrayList<>();

    @Override
    public void accept(Incident incident) {
        incidents.add(incident);
    }

    public List<IncidentType> types() {
        return incidents.stream().map(Incident::type).toList();
    }

    public Incident last() {
        return incidents.get(incidents.size() - 1);
    }

    public Incident lastOf(IncidentType type) {
        for (int i = incidents.size() - 1; i >= 0; i--) {
            if (incidents.get(i).type() == type) {
                return incidents.get(i);
            }
        }
        throw new AssertionError("No incident of type " + type + " in " + types());
    }
}
