package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class PeriodicAppointmentBooking {

    private UUID appointmentUid;
    private PeriodicPatternType patternType;
    private String pattern;
    private int ttl;
    private String payload;
    private HashMap<String, Object> extraParams;
    private int keyIteration;
    private boolean skipMissed;
    private long startTimestamp;
    private boolean startOnPeriodMultiple;
    private int startOnPeriodMultipleOffset;

    public PeriodicAppointmentBooking() {
    }

    public PeriodicAppointmentBooking
            (UUID appointmentUid,
             PeriodicPatternType patternType,
             String pattern,
             int ttl,
             String payload,
             HashMap<String, Object> extraParams,
             int keyIteration,
             boolean skipMissed,
             long startTimestamp,
             boolean startOnPeriodMultiple,
             int startOnPeriodMultipleOffset) {

        this.appointmentUid = appointmentUid;
        this.patternType = patternType;
        this.pattern = pattern;
        this.ttl = ttl;
        this.payload = payload;
        this.extraParams = extraParams;
        this.keyIteration = keyIteration;
        this.skipMissed = skipMissed;
        this.startTimestamp = startTimestamp;
        this.startOnPeriodMultiple = startOnPeriodMultiple;
        this.startOnPeriodMultipleOffset = startOnPeriodMultipleOffset;
    }

    public UUID getAppointmentUid() {
        return appointmentUid;
    }

    public PeriodicPatternType getPatternType() {
        return patternType;
    }

    public String getPattern() {
        return pattern;
    }

    public int getTtl() {
        return ttl;
    }

    public String getPayload() {
        return payload;
    }

    public HashMap<String, Object> getExtraParams() {
        return extraParams;
    }

    public int getKeyIteration() {
        return keyIteration;
    }

    public boolean isSkipMissed() {
        return skipMissed;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public boolean isStartOnPeriodMultiple() {
        return startOnPeriodMultiple;
    }

    public int getStartOnPeriodMultipleOffset() {
        return startOnPeriodMultipleOffset;
    }

    void setAppointmentUid(UUID appointmentUid) {
        this.appointmentUid = appointmentUid;
    }

    void setPatternType(PeriodicPatternType patternType) {
        this.patternType = patternType;
    }

    void setPattern(String pattern) {
        this.pattern = pattern;
    }

    void setTtl(int ttl) {
        this.ttl = ttl;
    }

    void setPayload(String payload) {
        this.payload = payload;
    }

    void setExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    void setKeyIteration(int keyIteration) {
        this.keyIteration = keyIteration;
    }

    void setSkipMissed(boolean skipMissed) {
        this.skipMissed = skipMissed;
    }

    void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    void setStartOnPeriodMultiple(boolean startOnPeriodMultiple) {
        this.startOnPeriodMultiple = startOnPeriodMultiple;
    }

    void setStartOnPeriodMultipleOffset(int startOnPeriodMultipleOffset) {
        this.startOnPeriodMultipleOffset = startOnPeriodMultipleOffset;
    }
}
