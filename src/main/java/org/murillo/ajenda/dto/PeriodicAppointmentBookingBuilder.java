package org.murillo.ajenda.dto;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import org.murillo.ajenda.utils.UUIDType5;

import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;

public final class PeriodicAppointmentBookingBuilder {
    private UUID appointmentUid;
    private PeriodicPatternType patternType;
    private long startTimestamp;
    private String pattern;
    private String payload;
    private HashMap<String, Object> extraParams = new HashMap<>();
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

    public PeriodicAppointmentBookingBuilder withHashUid() {
        this.appointmentUid = null;
        return this;
    }

    public PeriodicAppointmentBookingBuilder startingOnTimestamp(long startTimestamp) {
        if (startTimestamp <= 0) throw new IllegalArgumentException("startTimestamp must be a positive long");
        this.startTimestamp = startTimestamp;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withInitialDelay(long initialDelayMs) {
        if (initialDelayMs < 0) throw new IllegalArgumentException("initialDelayMs must not be a negative long");
        this.startTimestamp = -initialDelayMs;
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
        this.pattern = String.valueOf(period);
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

    public PeriodicAppointmentBookingBuilder withPayload(String payload) {
        this.payload = payload;
        return this;
    }

    public PeriodicAppointmentBookingBuilder withExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
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
        if (!skipMissed && Objects.equals(PeriodicPatternType.FIXED_DELAY, patternType))
            throw new IllegalArgumentException("FIXED_DELAY type is incompatible with disabled skipMissed option");

        PeriodicAppointmentBooking periodicAppointmentBooking = new PeriodicAppointmentBooking();
        periodicAppointmentBooking.setAppointmentUid(
                appointmentUid != null ?
                        appointmentUid
                        : UUIDType5.nameUUIDFromCustomString(payload + "_" + patternType.name() + "_" + pattern));
        periodicAppointmentBooking.setPatternType(patternType);
        periodicAppointmentBooking.setPattern(pattern);
        periodicAppointmentBooking.setPayload(payload);
        periodicAppointmentBooking.setExtraParams(extraParams);
        periodicAppointmentBooking.setSkipMissed(skipMissed);
        periodicAppointmentBooking.setStartTimestamp(startTimestamp);

        return periodicAppointmentBooking;
    }
}
