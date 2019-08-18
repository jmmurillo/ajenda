package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class PeriodicAppointmentBooking {

    private UUID appointmentUid;
    private PeriodicPatternType patternType;
    private String pattern;
    private String payload;
    private HashMap<String, Object> extraParams;
    private boolean skipMissed;
    private long startTimestamp;

    public PeriodicAppointmentBooking() {
    }

    public PeriodicAppointmentBooking
            (UUID appointmentUid,
             PeriodicPatternType patternType,
             String pattern,
             String payload,
             HashMap<String, Object> extraParams,
             boolean skipMissed,
             long startTimestamp) {

        this.appointmentUid = appointmentUid;
        this.patternType = patternType;
        this.pattern = pattern;
        this.payload = payload;
        this.extraParams = extraParams;
        this.skipMissed = skipMissed;
        this.startTimestamp = startTimestamp;
    }

    public UUID getAppointmentUid() {
        return appointmentUid;
    }

    public void setAppointmentUid(UUID appointmentUid) {
        this.appointmentUid = appointmentUid;
    }

    public PeriodicPatternType getPatternType() {
        return patternType;
    }

    public void setPatternType(PeriodicPatternType patternType) {
        this.patternType = patternType;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public HashMap<String, Object> getExtraParams() {
        return extraParams;
    }

    public void setExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    public boolean isSkipMissed() {
        return skipMissed;
    }

    public void setSkipMissed(boolean skipMissed) {
        this.skipMissed = skipMissed;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }
}
