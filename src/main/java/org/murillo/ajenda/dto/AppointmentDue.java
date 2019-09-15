package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class AppointmentDue {

    private UUID appointmentUid;
    private long dueTimestamp;
    private int ttl;
    private String payload;
    private int attempts;
    private HashMap<String, ?> extraParams;
    private UUID periodicAppointmentUid;
    private int flags;

    public AppointmentDue(
            UUID appointmentUid,
            long dueTimestamp,
            int ttl,
            String payload,
            int attempts,
            HashMap<String, ?> extraParams,
            UUID periodicAppointmentUid,
            int flags
    ) {
        this.appointmentUid = appointmentUid;
        this.dueTimestamp = dueTimestamp;
        this.ttl = ttl;
        this.payload = payload;
        this.attempts = attempts;
        this.extraParams = extraParams;
        this.periodicAppointmentUid = periodicAppointmentUid;
        this.flags = flags;
    }

    public UUID getAppointmentUid() {
        return appointmentUid;
    }

    public long getDueTimestamp() {
        return dueTimestamp;
    }

    public int getTtl() {
        return ttl;
    }

    public String getPayload() {
        return payload;
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

    public int getFlags() {
        return flags;
    }

    void setAppointmentUid(UUID appointmentUid) {
        this.appointmentUid = appointmentUid;
    }

    void setDueTimestamp(long dueTimestamp) {
        this.dueTimestamp = dueTimestamp;
    }

    void setTtl(int ttl) {
        this.ttl = ttl;
    }

    void setPayload(String payload) {
        this.payload = payload;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    void setExtraParams(HashMap<String, ?> extraParams) {
        this.extraParams = extraParams;
    }

    void setPeriodicAppointmentUid(UUID periodicAppointmentUid) {
        this.periodicAppointmentUid = periodicAppointmentUid;
    }

    void setFlags(int flags) {
        this.flags = flags;
    }

    @Override
    public String toString() {
        return "AppointmentDue{" +
                "appointmentUid=" + appointmentUid +
                ", dueTimestamp=" + dueTimestamp +
                ", payload='" + payload + '\'' +
                ", attempts=" + attempts +
                ", extraParams=" + extraParams +
                ", periodicAppointmentUid=" + periodicAppointmentUid +
                ", flags=" + flags +
                '}';
    }
}
