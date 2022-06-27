package org.murillo.ajenda.dto;

public class UnhandledAppointmentException extends Exception {

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
