package org.murillo.ajenda.dto;

import org.murillo.ajenda.CancelFlag;

public interface CancellableAppointmentListener {
    
    void receive(AppointmentDue appointmentDue, CancelFlag cancelFlag) throws Exception;
    
}
