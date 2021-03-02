package org.murillo.ajenda.core;

import java.util.Objects;
import java.util.UUID;

public class CancelledResult {
    private UUID uuid;
    private UUID periodicUuid;
    private long dueDate;
    private long timeoutDate;

    public CancelledResult(UUID uuid, UUID periodicUuid, long dueDate, long timeoutDate) {
        this.uuid = uuid;
        this.periodicUuid = periodicUuid;
        this.dueDate = dueDate;
        this.timeoutDate = timeoutDate;
    }

    public UUID getUuid() {
        return uuid;
    }

    public UUID getPeriodicUuid() {
        return periodicUuid;
    }

    public long getDueDate() {
        return dueDate;
    }

    public long getTimeoutDate() {
        return timeoutDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CancelledResult that = (CancelledResult) o;
        return dueDate == that.dueDate &&
                timeoutDate == that.timeoutDate &&
                Objects.equals(uuid, that.uuid) &&
                Objects.equals(periodicUuid, that.periodicUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid, periodicUuid, dueDate, timeoutDate);
    }
}
