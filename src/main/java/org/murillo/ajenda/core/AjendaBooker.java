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

    void cancel(Connection connection, UUID... uuids) throws Exception;

    void cancel(Session session, UUID... uuids) throws Exception;

    void cancel(List<UUID> uuids) throws Exception;

    void cancel(Connection connection, List<UUID> uuids) throws Exception;

    void cancel(Session session, List<UUID> uuids) throws Exception;

    void cancelPeriodic(UUID... periodic_uuids) throws Exception;

    void cancelPeriodic(Connection connection, UUID... periodic_uuids) throws Exception;

    void cancelPeriodic(Session session, UUID... periodic_uuids) throws Exception;

    void cancelPeriodic(List<UUID> periodic_uuids) throws Exception;

    void cancelPeriodic(Connection connection, List<UUID> periodic_uuids) throws Exception;

    void cancelPeriodic(Session session, List<UUID> periodic_uuids) throws Exception;

    void cancelKeys(String... keys) throws Exception;

    void cancelKeys(Connection connection, String... keys) throws Exception;

    void cancelKeys(Session session, String... keys) throws Exception;

    void cancelKeys(List<String> keys) throws Exception;

    void cancelKeys(Connection connection, List<String> keys) throws Exception;

    void cancelKeys(Session session, List<String> keys) throws Exception;

    void cancelPeriodicKeys(String... periodic_keys) throws Exception;

    void cancelPeriodicKeys(Connection connection, String... periodic_keys) throws Exception;

    void cancelPeriodicKeys(Session session, String... periodic_keys) throws Exception;

    void cancelPeriodicKeys(List<String> periodic_keys) throws Exception;

    void cancelPeriodicKeys(Connection connection, List<String> periodic_keys) throws Exception;

    void cancelPeriodicKeys(Session session, List<String> periodic_keys) throws Exception;
}
