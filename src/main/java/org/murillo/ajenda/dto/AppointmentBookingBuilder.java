package org.murillo.ajenda.dto;

import org.murillo.ajenda.utils.UUIDType5;

import java.util.HashMap;
import java.util.UUID;

public final class AppointmentBookingBuilder {

    private static final UUID UUID_ZERO = new UUID(0L, 0L);

    private UUID appointmentUid;
    private long dueTimestamp = 0;
    private int ttl = 0;
    private String payload;
    private HashMap<String, Object> extraParams = new HashMap<>();

    private AppointmentBookingBuilder() {
    }

    public static AppointmentBookingBuilder aBooking() {
        return new AppointmentBookingBuilder();
    }

    public AppointmentBookingBuilder withUid(UUID appointmentUid) {
        if (appointmentUid == null) throw new IllegalArgumentException("appointmentUid must not be null");
        this.appointmentUid = appointmentUid;
        return this;
    }

    public AppointmentBookingBuilder withHashUid(String key) {
        this.appointmentUid = UUIDType5.nameUUIDFromCustomString(key);
        return this;
    }
    
    public AppointmentBookingBuilder withHashUid() {
        this.appointmentUid = null;
        return this;
    }

    public AppointmentBookingBuilder withDueTimestamp(long dueTimestamp) {
        if (dueTimestamp <= 0) throw new IllegalArgumentException("dueTimestamp must be a positive long");
        this.dueTimestamp = dueTimestamp;
        return this;
    }

    public AppointmentBookingBuilder withDelayedDue(long delayMs) {
        if (delayMs < 0) throw new IllegalArgumentException("delayMs must not be a negative long");
        this.dueTimestamp = -delayMs;
        return this;
    }

    public AppointmentBookingBuilder withImmediateDue() {
        this.dueTimestamp = 0L;
        return this;
    }

    public AppointmentBookingBuilder withTtl(int ttl){
        if(ttl <= 0) this.ttl = 0;
        else this.ttl = ttl;
        return this;
    }

    public AppointmentBookingBuilder withPayload(String payload) {
        this.payload = payload;
        return this;
    }

    public AppointmentBookingBuilder withExtraParam(String columnName, Object value) {
        this.extraParams.put(columnName, value);
        return this;
    }

    public AppointmentBooking build() {
        AppointmentBooking appointmentBooking = new AppointmentBooking();
        appointmentBooking.setDueTimestamp(dueTimestamp);
        appointmentBooking.setTtl(ttl);
        appointmentBooking.setPayload(payload);
        appointmentBooking.setAppointmentUid(
                appointmentUid != null ?
                        appointmentUid
                        : UUIDType5.nameUUIDFromCustomString(payload + "_" + dueTimestamp));
        if (this.extraParams != null && !extraParams.isEmpty()) {
            appointmentBooking.setExtraParams(this.extraParams);
        }

        return appointmentBooking;
    }
}
