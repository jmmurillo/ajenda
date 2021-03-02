package org.murillo.ajenda.dto;

public interface SimpleAppointmentListener {

    void receive(AppointmentDue appointmentDue) throws Exception;

}
