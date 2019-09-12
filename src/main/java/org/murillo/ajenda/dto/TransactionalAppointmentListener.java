package org.murillo.ajenda.dto;

import org.murillo.ajenda.CancelFlag;

import java.sql.Connection;

public interface TransactionalAppointmentListener<T extends Connection> {
    
    void receive(AppointmentDue appointmentDue, CancelFlag cancelFlag, AjendaBooker<T> transactionalBooker) throws UnhandledAppointmentException;

}
