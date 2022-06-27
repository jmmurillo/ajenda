package org.murillo.ajenda.dto;

import java.util.UUID;

public class ConflictingPeriodicBookingException extends Exception {

    private final UUID conflictingPeriodicAppointmentUid;

    public ConflictingPeriodicBookingException(UUID conflictingPeriodicAppointmentUid) {
        super(String.valueOf(conflictingPeriodicAppointmentUid));
        this.conflictingPeriodicAppointmentUid = conflictingPeriodicAppointmentUid;
    }

    public UUID getConflictingPeriodicAppointmentUid() {
        return conflictingPeriodicAppointmentUid;
    }
}
