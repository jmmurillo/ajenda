package org.murillo.ajenda.dto;

public interface CancellableAppointmentListener {
    
    void receive(AppointmentDue appointmentDue, CancelFlag cancelFlag) throws Exception;
    
}
