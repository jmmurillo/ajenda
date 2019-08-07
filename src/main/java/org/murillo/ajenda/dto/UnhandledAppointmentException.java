package org.murillo.ajenda.dto;

public class UnhandledAppointmentException extends Exception {
    public UnhandledAppointmentException() {
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
