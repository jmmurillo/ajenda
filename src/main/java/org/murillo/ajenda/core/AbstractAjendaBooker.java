package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;
import org.murillo.ajenda.utils.UUIDType5;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.murillo.ajenda.core.Common.getPeriodicTableNameForTopic;

public abstract class AbstractAjendaBooker implements AjendaBooker {

    protected final ConnectionFactory dataSource;
    protected final String topic;
    protected final String schemaName;
    protected final String tableName;
    protected final String periodicTableName;
    protected final Clock clock;

    protected AbstractAjendaBooker(ConnectionFactory dataSource, String topic, String schemaName, Clock clock) {
        if (dataSource == null) throw new IllegalArgumentException("dataSource must not be null");
        if (clock == null) throw new IllegalArgumentException("clock must not be null");
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic must not be empty");
        if (schemaName == null || schemaName.isEmpty())
            throw new IllegalArgumentException("schema name must not be empty");

        this.dataSource = dataSource;
        this.topic = topic;
        this.schemaName = schemaName;
        this.tableName = Common.getTableNameForTopic(topic);
        this.periodicTableName = getPeriodicTableNameForTopic(topic);
        this.clock = clock;
    }

    protected AbstractAjendaBooker(AjendaScheduler ajendaScheduler) {
        this.dataSource = ajendaScheduler;
        this.clock = ajendaScheduler.getClock();
        this.topic = ajendaScheduler.getTopic();
        this.schemaName = ajendaScheduler.getSchemaName();
        this.tableName = ajendaScheduler.getTableName();
        this.periodicTableName = ajendaScheduler.getPeriodicTableName();
    }

    @Override
    public abstract ConnectionWrapper getConnection() throws Exception;

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public String getTableNameWithSchema() {
        return '\"' + schemaName + "\"." + tableName;
    }

    @Override
    public String getPeriodicTableNameWithSchema() {
        return '\"' + schemaName + "\"." + periodicTableName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getPeriodicTableName() {
        return periodicTableName;
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public void book(AppointmentBooking... bookings) throws Exception {
        book(Arrays.asList(bookings));
    }

    @Override
    public void book(Session session, AppointmentBooking... bookings) throws Exception {
        book(session, Arrays.asList(bookings));
    }

    @Override
    public void book(Connection connection, AppointmentBooking... bookings) throws Exception {
        book(connection, Arrays.asList(bookings));
    }

    @Override
    public void book(List<AppointmentBooking> bookings) throws Exception {
        BookModel.book(
                this.getTableNameWithSchema(),
                this,
                this.getClock(),
                0,
                bookings);
    }

    @Override
    public void book(Connection connection, List<AppointmentBooking> bookings) throws Exception {
        BookModel.book(
                this.getTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                this.getClock(),
                0,
                bookings);
    }

    @Override
    public void book(Session session, List<AppointmentBooking> bookings) throws Exception {
        BookModel.book(
                this.getTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                this.getClock(),
                0,
                bookings);
    }

    @Override
    public void bookPeriodic(
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception {
        bookPeriodic(conflictPolicy, Arrays.asList(bookings));
    }

    @Override
    public void bookPeriodic(
            Connection connection,
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception {
        bookPeriodic(connection, conflictPolicy, Arrays.asList(bookings));
    }

    @Override
    public void bookPeriodic(
            Session session,
            PeriodicBookConflictPolicy conflictPolicy,
            PeriodicAppointmentBooking... bookings) throws Exception {
        bookPeriodic(session, conflictPolicy, Arrays.asList(bookings));
    }


    @Override
    public void bookPeriodic(
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception {
        BookModel.bookPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                this,
                this.getClock(),
                bookings,
                conflictPolicy);
    }

    @Override
    public void bookPeriodic(
            Connection connection,
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception {
        BookModel.bookPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                this.getClock(),
                bookings,
                conflictPolicy);
    }

    @Override
    public void bookPeriodic(
            Session session,
            PeriodicBookConflictPolicy conflictPolicy,
            List<PeriodicAppointmentBooking> bookings) throws Exception {
        BookModel.bookPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                this.getClock(),
                bookings,
                conflictPolicy);
    }

    @Override
    public void cancel(UUID... uuids) throws Exception {
        cancel(Arrays.asList(uuids));
    }

    @Override
    public void cancel(Connection connection, UUID... uuids) throws Exception {
        cancel(connection, Arrays.asList(uuids));
    }

    @Override
    public void cancel(Session session, UUID... uuids) throws Exception {
        cancel(session, Arrays.asList(uuids));
    }

    @Override
    public void cancel(List<UUID> uuids) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                this,
                uuids);
    }

    @Override
    public void cancel(Connection connection, List<UUID> uuids) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                uuids);
    }

    @Override
    public void cancel(Session session, List<UUID> uuids) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                uuids);
    }

    @Override
    public void cancelPeriodic(UUID... periodic_uuids) throws Exception {
        cancelPeriodic(Arrays.asList(periodic_uuids));
    }    
    
    @Override
    public void cancelPeriodic(Connection connection, UUID... periodic_uuids) throws Exception {
        cancelPeriodic(connection, Arrays.asList(periodic_uuids));
    }    
    
    @Override
    public void cancelPeriodic(Session session, UUID... periodic_uuids) throws Exception {
        cancelPeriodic(session, Arrays.asList(periodic_uuids));
    }

    @Override
    public void cancelPeriodic(List<UUID> periodic_uuids) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                this,
                periodic_uuids);
    }   
    
    @Override
    public void cancelPeriodic(Connection connection, List<UUID> periodic_uuids) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                periodic_uuids);
    }
    
    @Override
    public void cancelPeriodic(Session session, List<UUID> periodic_uuids) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                periodic_uuids);
    }    
    
    @Override
    public void cancelKeys(String... keys) throws Exception {
        cancelKeys(Arrays.asList(keys));
    }

    @Override
    public void cancelKeys(Connection connection, String... keys) throws Exception {
        cancelKeys(connection, Arrays.asList(keys));
    }

    @Override
    public void cancelKeys(Session session, String... keys) throws Exception {
        cancelKeys(session, Arrays.asList(keys));
    }

    @Override
    public void cancelKeys(List<String> keys) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                this,
                keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }

    @Override
    public void cancelKeys(Connection connection, List<String> keys) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }

    @Override
    public void cancelKeys(Session session, List<String> keys) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }

    @Override
    public void cancelPeriodicKeys(String... periodic_keys) throws Exception {
        cancelPeriodicKeys(Arrays.asList(periodic_keys));
    }    
    
    @Override
    public void cancelPeriodicKeys(Connection connection, String... periodic_keys) throws Exception {
        cancelPeriodicKeys(connection, Arrays.asList(periodic_keys));
    }    
    
    @Override
    public void cancelPeriodicKeys(Session session, String... periodic_keys) throws Exception {
        cancelPeriodicKeys(session, Arrays.asList(periodic_keys));
    }

    @Override
    public void cancelPeriodicKeys(List<String> periodic_keys) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                this,
                periodic_keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }   
    
    @Override
    public void cancelPeriodicKeys(Connection connection, List<String> periodic_keys) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new JdbcConnectionWrapper(() -> connection).recursiveConnectionWrapper(),
                periodic_keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }
    
    @Override
    public void cancelPeriodicKeys(Session session, List<String> periodic_keys) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                () -> new HibernateConnectionWrapper(() -> session).recursiveConnectionWrapper(),
                periodic_keys.stream().map(UUIDType5::nameUUIDFromCustomString).collect(Collectors.toList()));
    }


}
