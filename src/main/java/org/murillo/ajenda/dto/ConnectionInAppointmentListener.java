package org.murillo.ajenda.dto;

import org.murillo.ajenda.booking.AjendaBooker;

public interface ConnectionInAppointmentListener {
    
    void receive(AppointmentDue appointmentDue, AjendaBooker ajendaBooker) throws UnhandledAppointmentException;

}
