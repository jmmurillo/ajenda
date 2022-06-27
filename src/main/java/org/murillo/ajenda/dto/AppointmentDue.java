package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class AppointmentDue {

    private UUID appointmentUid;
    private long dueTimestamp;
    private int ttl;
    private String payload;
    private int attempts;
    private HashMap<String, Object> extraParams;
    private UUID periodicAppointmentUid;
    private int flags;
    private short version;

    public AppointmentDue(
            UUID appointmentUid,
            long dueTimestamp,
            int ttl,
            String payload,
            int attempts,
            HashMap<String, Object> extraParams,
            UUID periodicAppointmentUid,
            int flags,
            short version) {
        this.appointmentUid = appointmentUid;
        this.dueTimestamp = dueTimestamp;
        this.ttl = ttl;
        this.payload = payload;
        this.attempts = attempts;
        this.extraParams = extraParams;
        this.periodicAppointmentUid = periodicAppointmentUid;
        this.flags = flags;
        this.version = version;
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

    public HashMap<String, Object> getExtraParams() {
        return extraParams;
    }

    public UUID getPeriodicAppointmentUid() {
        return periodicAppointmentUid;
    }

    public int getFlags() {
        return flags;
    }

    public short getVersion() {
        return version;
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

    void setExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    void setPeriodicAppointmentUid(UUID periodicAppointmentUid) {
        this.periodicAppointmentUid = periodicAppointmentUid;
    }

    void setFlags(int flags) {
        this.flags = flags;
    }

    void setVersion(short version) {
        this.version = version;
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
                ", version=" + version +
                '}';
    }
}
