package org.murillo.ajenda.dto;

import org.murillo.ajenda.booking.AjendaBooker;

import java.sql.Connection;

public interface ConnectionInAppointmentListener<T extends Connection> {
    
    void receive(AppointmentDue appointmentDue, AjendaBooker<T> ajendaBooker) throws UnhandledAppointmentException;

}
