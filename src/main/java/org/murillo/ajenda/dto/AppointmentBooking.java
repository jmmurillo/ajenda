package org.murillo.ajenda.dto;

import java.util.HashMap;
import java.util.UUID;

public class AppointmentBooking {

    private UUID appointmentUid;
    private long dueTimestamp;
    private String payload;
    private HashMap<String, Object> extraParams;

    AppointmentBooking(){}

    public UUID getAppointmentUid() {
        return appointmentUid;
    }

    void setAppointmentUid(UUID appointmentUid) {
        this.appointmentUid = appointmentUid;
    }

    public long getDueTimestamp() {
        return dueTimestamp;
    }

    void setDueTimestamp(long dueTimestamp) {
        this.dueTimestamp = dueTimestamp;
    }

    public String getPayload() {
        return payload;
    }

    void setPayload(String payload) {
        this.payload = payload;
    }

    public HashMap<String, Object> getExtraParams() {
        return extraParams;
    }

    void setExtraParams(HashMap<String, Object> extraParams) {
        this.extraParams = extraParams;
    }
}
