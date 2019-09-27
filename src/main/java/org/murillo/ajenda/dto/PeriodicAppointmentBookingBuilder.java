package org.murillo.ajenda.dto;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.murillo.ajenda.utils.UUIDType5;

import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;

public final class PeriodicAppointmentBookingBuilder {
    public static final int MAX_KEY_ITERATION = 1000;

    private UUID appointmentUid;
    private PeriodicPatternType patternType;
    private long startTimestamp = 0;
    private boolean startOnMultiple = false;
    private String pattern;
    private int ttl;
    private String payload;
    private HashMap<String, Object> extraParams = new HashMap<>();
    private int keyIteration = 1;
    private boolean skipMissed = true;

    private PeriodicAppointmentBookingBuilder() {
    }

    public static PeriodicAppointmentBookingBuilder aPeriodicBooking() {
        return new PeriodicAppointmentBookingBuilder();
    }

    public PeriodicAppointmentBookingBuilder withUid(UUID appointmentUid) {
        if (appointmentUid == null) throw new IllegalArgumentException("appointmentUid must not be null");
        this.appointmentUid = appointmentUid;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withHashUid(String key) {
        this.appointmentUid = UUIDType5.nameUUIDFromCustomString(key);
        return this;
    }    
    
    public PeriodicAppointmentBookingBuilder withHashUid() {
        this.appointmentUid = null;
        return this;
    }

    public PeriodicAppointmentBookingBuilder startingOnTimestamp(long startTimestamp) {
        if (startTimestamp <= 0) throw new IllegalArgumentException("startTimestamp must be a positive long");
        this.startTimestamp = startTimestamp;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withDelayedStart(long delayMs) {
        if (delayMs < 0) throw new IllegalArgumentException("delayMs must not be a negative long");
        this.startTimestamp = -delayMs;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withImmediateStart() {
        this.startTimestamp = 0L;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withStartOnPeriodMultiple(boolean startOnMultiple){
        this.startOnMultiple = startOnMultiple;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withFixedPeriod(long period, PeriodicPatternType patternType) {
        if (patternType == null) throw new IllegalArgumentException("patternType must not be null");
        this.patternType = patternType;
        switch (patternType) {
            case FIXED_RATE:
                break;
            case FIXED_DELAY:
                break;
            default:
                throw new IllegalArgumentException("patternType " + patternType + " is not a simple period sub-type");
        }
        this.pattern = Long.toHexString(period);
        return this;
    }

    public PeriodicAppointmentBookingBuilder withCronPattern(String pattern, PeriodicPatternType patternType) {
        if (pattern == null) throw new IllegalArgumentException("pattern must not be null");
        if (patternType == null) throw new IllegalArgumentException("patternType must not be null");
        this.patternType = patternType;
        CronType cronType;
        switch (patternType) {
            case CRON_UNIX:
                cronType = CronType.UNIX;
                break;
            case CRON_CRON4J:
                cronType = CronType.CRON4J;
                break;
            case CRON_QUARTZ:
                cronType = CronType.QUARTZ;
                break;
            case CRON_SPRING:
                cronType = CronType.SPRING;
                break;
            default:
                throw new IllegalArgumentException("patternType " + patternType + " is not a Cron sub-type");
        }
        //Throws IllegalArgumentException if invalid pattern
        new CronParser(CronDefinitionBuilder.instanceDefinitionFor(cronType)).parse(pattern).validate();
        this.pattern = pattern;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withTtl(int ttl){
        if(ttl <= 0) this.ttl = 0;
        else this.ttl = ttl;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withPayload(String payload) {
        this.payload = payload;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withKeyIteration(int keyIteration) {
        if (keyIteration > MAX_KEY_ITERATION
        || keyIteration < 1) throw new IllegalArgumentException("keyIteration must be a positive integer lower than " + MAX_KEY_ITERATION);
        this.keyIteration = keyIteration;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withSkipMissed(boolean skipMissed) {
        this.skipMissed = skipMissed;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withExtraParam(String columnName, Object value) {
        this.extraParams.put(columnName, value);
        return this;
    }

    public PeriodicAppointmentBooking build() {

        if (patternType == null) throw new IllegalArgumentException("patternType must not be null");
        if (pattern == null) throw new IllegalArgumentException("pattern must not be null");
        if (Objects.equals(PeriodicPatternType.FIXED_DELAY, patternType)) skipMissed = false;

        PeriodicAppointmentBooking periodicAppointmentBooking = new PeriodicAppointmentBooking();
        periodicAppointmentBooking.setAppointmentUid(
                appointmentUid != null ?
                        appointmentUid
                        : UUIDType5.nameUUIDFromCustomString(payload + "_" + patternType.name() + "_" + pattern));
        periodicAppointmentBooking.setPatternType(patternType);
        periodicAppointmentBooking.setPattern(pattern);
        periodicAppointmentBooking.setTtl(ttl);
        periodicAppointmentBooking.setPayload(payload);
        periodicAppointmentBooking.setExtraParams(extraParams);
        periodicAppointmentBooking.setKeyIteration(keyIteration);
        periodicAppointmentBooking.setSkipMissed(skipMissed);
        periodicAppointmentBooking.setStartTimestamp(startTimestamp);
        periodicAppointmentBooking.setStartOnPeriodMultiple(startOnMultiple);

        return periodicAppointmentBooking;
    }
}
