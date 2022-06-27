package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;
import org.murillo.ajenda.utils.UUIDType5;

import java.sql.Connection;
import java.util.*;
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
    public Map<UUID, CancelledResult> cancel(ConnectionFactory connectionFactory, List<UUID> uuids) throws Exception {
        Map<UUID, CancelledResult> cancelResults = BookModel.cancel(
                this.getTableNameWithSchema(),
                connectionFactory,
                uuids);
        cancelInQueue(uuids, cancelResults);
        return cancelResults;
    }

    @Override
    public Map<UUID, CancelledResult> cancelPeriodic(ConnectionFactory connectionFactory, List<UUID> periodicUuids) throws Exception {
        Map<UUID, CancelledResult> cancelResults = BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                connectionFactory,
                periodicUuids);
        periodicCancelInQueue(periodicUuids, cancelResults);
        return cancelResults;
    }

    @Override
    public Map<String, CancelledResult> cancelKeys(ConnectionFactory connectionFactory, List<String> keys) throws Exception {
        Map<UUID, String> uuidsMap = keys.stream()
                .collect(Collectors.toMap(
                        UUIDType5::nameUUIDFromCustomString,
                        Function.identity(),
                        (old, neu) -> old,
                        HashMap::new));

        ArrayList<UUID> uuidsList = new ArrayList<>(uuidsMap.keySet());
        Map<UUID, CancelledResult> cancelResults = BookModel.cancel(
                this.getTableNameWithSchema(),
                connectionFactory,
                uuidsList);
        cancelInQueue(uuidsList, cancelResults);
        return cancelResults.entrySet().stream().collect(Collectors.toMap(e -> uuidsMap.get(e.getKey()), Map.Entry::getValue, (old, neu) -> old));
    }

    @Override
    public Map<String, CancelledResult> cancelPeriodicKeys(ConnectionFactory connectionFactory, List<String> periodicKeys) throws Exception {
        Map<UUID, String> periodicUuidsMap = periodicKeys.stream()
                .collect(Collectors.toMap(
                        UUIDType5::nameUUIDFromCustomString,
                        Function.identity(),
                        (old, neu) -> old,
                        HashMap::new));

        ArrayList<UUID> periodicUuidsList = new ArrayList<>(periodicUuidsMap.keySet());
        Map<UUID, CancelledResult> cancelResults = BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                connectionFactory,
                periodicUuidsList);
        periodicCancelInQueue(periodicUuidsList, cancelResults);
        return cancelResults.entrySet().stream().collect(Collectors.toMap(e -> periodicUuidsMap.get(e.getKey()), Map.Entry::getValue, (old, neu) -> old));
    }

    protected void cancelInQueue(List<UUID> uuids, Map<UUID, CancelledResult> cancelledResultMap) {
    }

    protected void periodicCancelInQueue(List<UUID> periodicUuids, Map<UUID, CancelledResult> cancelledResultMap) {
    }

}
