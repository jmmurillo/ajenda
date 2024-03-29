package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    default Map<UUID, CancelledResult> cancel(UUID... uuids) throws Exception {
        return cancel(Arrays.asList(uuids));
    }


    default Map<UUID, CancelledResult> cancel(Connection connection, UUID... uuids) throws Exception {
        return cancel(connection, Arrays.asList(uuids));
    }


    default void cancel(Session session, UUID... uuids) throws Exception {
        cancel(session, Arrays.asList(uuids));
    }


    default Map<UUID, CancelledResult> cancelPeriodic(UUID... periodicUuids) throws Exception {
        return cancelPeriodic(Arrays.asList(periodicUuids));
    }


    default Map<UUID, CancelledResult> cancelPeriodic(Connection connection, UUID... periodicUuids) throws Exception {
        return cancelPeriodic(connection, Arrays.asList(periodicUuids));
    }


    default Map<UUID, CancelledResult> cancelPeriodic(Session session, UUID... periodicUuids) throws Exception {
        return cancelPeriodic(session, Arrays.asList(periodicUuids));
    }


    default Map<String, CancelledResult> cancelKeys(String... keys) throws Exception {
        return cancelKeys(Arrays.asList(keys));
    }


    default Map<String, CancelledResult> cancelKeys(Connection connection, String... keys) throws Exception {
        return cancelKeys(connection, Arrays.asList(keys));
    }


    default Map<String, CancelledResult> cancelKeys(Session session, String... keys) throws Exception {
        return cancelKeys(session, Arrays.asList(keys));
    }


    default Map<String, CancelledResult> cancelPeriodicKeys(String... periodicKeys) throws Exception {
        return cancelPeriodicKeys(Arrays.asList(periodicKeys));
    }


    default Map<String, CancelledResult> cancelPeriodicKeys(Connection connection, String... periodicKeys) throws Exception {
        return cancelPeriodicKeys(connection, Arrays.asList(periodicKeys));
    }


    default Map<String, CancelledResult> cancelPeriodicKeys(Session session, String... periodicKeys) throws Exception {
        return cancelPeriodicKeys(session, Arrays.asList(periodicKeys));
    }


    default Map<UUID, CancelledResult> cancel(List<UUID> uuids) throws Exception {
        return cancel(this,
                uuids);
    }


    default Map<UUID, CancelledResult> cancel(Connection connection, List<UUID> uuids) throws Exception {
        return cancel(() -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                uuids);
    }


    default Map<UUID, CancelledResult> cancel(Session session, List<UUID> uuids) throws Exception {
        return cancel(() -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                uuids);
    }


    Map<UUID, CancelledResult> cancel(ConnectionFactory connectionFactory, List<UUID> uuids) throws Exception;


    default Map<UUID, CancelledResult> cancelPeriodic(List<UUID> periodicUuids) throws Exception {
        return cancelPeriodic(this,
                periodicUuids);
    }


    default Map<UUID, CancelledResult> cancelPeriodic(Connection connection, List<UUID> periodicUuids) throws Exception {
        return cancelPeriodic(() -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                periodicUuids);
    }


    default Map<UUID, CancelledResult> cancelPeriodic(Session session, List<UUID> periodicUuids) throws Exception {
        return cancelPeriodic(() -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                periodicUuids);
    }


    Map<UUID, CancelledResult> cancelPeriodic(ConnectionFactory connectionFactory, List<UUID> periodicUuids) throws Exception;


    default Map<String, CancelledResult> cancelKeys(List<String> keys) throws Exception {
        return cancelKeys(this,
                keys);
    }


    default Map<String, CancelledResult> cancelKeys(Connection connection, List<String> keys) throws Exception {
        return cancelKeys(() -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                keys);
    }


    default Map<String, CancelledResult> cancelKeys(Session session, List<String> keys) throws Exception {
        return cancelKeys(() -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                keys);
    }


    Map<String, CancelledResult> cancelKeys(ConnectionFactory connectionFactory, List<String> keys) throws Exception;


    default Map<String, CancelledResult> cancelPeriodicKeys(List<String> periodicKeys) throws Exception {
        return cancelPeriodicKeys(this,
                periodicKeys);
    }


    default Map<String, CancelledResult> cancelPeriodicKeys(Connection connection, List<String> periodicKeys) throws Exception {
        return cancelPeriodicKeys(() -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                periodicKeys);
    }


    default Map<String, CancelledResult> cancelPeriodicKeys(Session session, List<String> periodicKeys) throws Exception {
        return cancelPeriodicKeys(() -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                periodicKeys);
    }


    Map<String, CancelledResult> cancelPeriodicKeys(ConnectionFactory connectionFactory, List<String> periodicKeys) throws Exception;
}
