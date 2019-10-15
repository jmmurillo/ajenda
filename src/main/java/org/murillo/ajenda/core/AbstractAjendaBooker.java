package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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
    public void book(List<AppointmentBooking> bookings) throws Exception {
        BookModel.book(
                this.getTableNameWithSchema(),
                this,
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
    public void cancel(UUID... uuids) throws Exception {
        cancel(Arrays.asList(uuids));
    }

    @Override
    public void cancel(List<UUID> uuids) throws Exception {
        BookModel.cancel(
                this.getTableNameWithSchema(),
                this,
                uuids);
    }

    @Override
    public void cancelPeriodic(UUID... periodic_uuids) throws Exception {
        cancelPeriodic(Arrays.asList(periodic_uuids));
    }

    @Override
    public void cancelPeriodic(List<UUID> periodic_uuids) throws Exception {
        BookModel.cancelPeriodic(
                this.getTableNameWithSchema(),
                this.getPeriodicTableNameWithSchema(),
                this,
                periodic_uuids);
    }


}
