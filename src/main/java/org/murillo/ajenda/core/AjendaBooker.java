package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;

import java.sql.Connection;
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

    void book(Session session, AppointmentBooking... bookings) throws Exception;

    void book(Connection connection, AppointmentBooking... bookings) throws Exception;

    void book(List<AppointmentBooking> bookings) throws Exception;

    abstract void book(Connection connection, List<AppointmentBooking> bookings) throws Exception;

    abstract void book(Session session, List<AppointmentBooking> bookings) throws Exception;

    void bookPeriodic(
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception;

    void bookPeriodic(
            Connection connection,
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception;

    void bookPeriodic(
            Session session,
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception;

    void bookPeriodic(
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception;

    void bookPeriodic(
            Connection connection,
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception;

    void bookPeriodic(
            Session session,
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception;

    void cancel(UUID... uuids) throws Exception;

    void cancel(List<UUID> uuids) throws Exception;

    void cancelPeriodic(UUID... periodic_uuids) throws Exception;

    void cancelPeriodic(List<UUID> periodic_uuids) throws Exception;
}
