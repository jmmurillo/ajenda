package org.murillo.ajenda.dto;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;

public interface AjendaBooker<T extends Connection> extends ConnectionFactory<T> {
    T getConnection() throws Exception;

    String getTopic();

    String getTableNameWithSchema();

    String getPeriodicTableNameWithSchema();

    String getSchemaName();

    String getTableName();

    String getPeriodicTableName();

    Clock getClock();

    void book(AppointmentBooking... bookings) throws Exception;

    void book(List<AppointmentBooking> bookings) throws Exception;

    void bookPeriodic(PeriodicAppointmentBooking... bookings) throws Exception;

    void bookPeriodic(List<PeriodicAppointmentBooking> bookings) throws Exception;

    void tryBookPeriodic(PeriodicAppointmentBooking... bookings) throws Exception;

    void tryBookPeriodic(List<PeriodicAppointmentBooking> bookings) throws Exception;

    void cancel(UUID... uuids) throws Exception;

    void cancel(List<UUID> uuids) throws Exception;

    void cancelPeriodic(UUID... periodic_uuids) throws Exception;

    void cancelPeriodic(List<UUID> periodic_uuids) throws Exception;
}
