package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class AppointmentDue {

    private UUID appointmentUid;
    private long dueTimestamp;
    private long readTimestamp;
    private String payload;
    private int attempts;
    private HashMap<String, ?> extraParams;
    private UUID periodicAppointmentUid;

    public AppointmentDue(
            UUID appointmentUid,
            long dueTimestamp,
            long readTimestamp,
            String payload,
            int attempts,
            HashMap<String, ?> extraParams,
            UUID periodicAppointmentUid
    ) {
        this.appointmentUid = appointmentUid;
        this.dueTimestamp = dueTimestamp;
        this.readTimestamp = readTimestamp;
        this.payload = payload;
        this.attempts = attempts;
        this.extraParams = extraParams;
        this.periodicAppointmentUid = periodicAppointmentUid;
    }

    public UUID getAppointmentUid() {
        return appointmentUid;
    }

    public long getDueTimestamp() {
        return dueTimestamp;
    }

    public String getPayload() {
        return payload;
    }

    public long getReadTimestamp() {
        return readTimestamp;
    }

    public int getAttempts() {
        return attempts;
    }

    public HashMap<String, ?> getExtraParams() {
        return extraParams;
    }

    public UUID getPeriodicAppointmentUid() {
        return periodicAppointmentUid;
    }

    public void setAppointmentUid(UUID appointmentUid) {
        this.appointmentUid = appointmentUid;
    }

    public void setDueTimestamp(long dueTimestamp) {
        this.dueTimestamp = dueTimestamp;
    }

    public void setReadTimestamp(long readTimestamp) {
        this.readTimestamp = readTimestamp;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public void setExtraParams(HashMap<String, ?> extraParams) {
        this.extraParams = extraParams;
    }

    public void setPeriodicAppointmentUid(UUID periodicAppointmentUid) {
        this.periodicAppointmentUid = periodicAppointmentUid;
    }

    @Override
    public String toString() {
        return "AppointmentDue{" +
                "appointmentUid=" + appointmentUid +
                ", dueTimestamp=" + dueTimestamp +
                ", readTimestamp=" + readTimestamp +
                ", payload='" + payload + '\'' +
                ", attempts=" + attempts +
                ", extraParams=" + extraParams +
                ", periodicAppointmentUid=" + periodicAppointmentUid +
                '}';
    }
}
