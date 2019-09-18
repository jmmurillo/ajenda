package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;

import java.util.List;
import java.util.UUID;

public interface AjendaBooker extends ConnectionFactory {

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
