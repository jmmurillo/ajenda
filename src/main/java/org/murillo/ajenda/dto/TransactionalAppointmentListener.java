package org.murillo.ajenda.dto;

import org.murillo.ajenda.core.AjendaBooker;

import java.sql.Connection;

public interface TransactionalAppointmentListener {

    void receive(AppointmentDue appointmentDue, CancelFlag cancelFlag, AjendaBooker transactionalBooker) throws UnhandledAppointmentException;

}
