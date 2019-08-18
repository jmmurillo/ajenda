package org.murillo.ajenda.booking;

import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.common.ConnectionFactory;
import org.murillo.ajenda.common.InitializationModel;
import org.murillo.ajenda.common.SyncedClock;
import org.murillo.ajenda.dto.AppointmentBooking;
import org.murillo.ajenda.handling.AjendaScheduler;
import org.murillo.ajenda.utils.Common;

import java.sql.Connection;

public class AjendaBooker<T extends Connection> implements ConnectionFactory<T> {

    public static final String DEFAULT_SCHEMA_NAME = "public";
    private final ConnectionFactory<T> dataSource;
    private final String topic;
    private final String schemaName;
    private final String tableName;
    private final Clock clock;

    public AjendaBooker(ConnectionFactory<T> dataSource, String topic) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), DEFAULT_SCHEMA_NAME);
    }

    public AjendaBooker(ConnectionFactory<T> dataSource, String topic,
                        String customSchema) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), customSchema);
    }

    public AjendaBooker(AjendaScheduler<T> ajendaScheduler) {
        this.dataSource = ajendaScheduler;
        this.clock = ajendaScheduler.getClock();
        this.topic = ajendaScheduler.getTopic();
        this.schemaName = ajendaScheduler.getSchemaName();
        this.tableName = ajendaScheduler.getTableNameWithSchema();
    }

    public AjendaBooker(ConnectionFactory<T> dataSource, String topic, Clock clock) throws Exception {
        this(dataSource, topic, clock, DEFAULT_SCHEMA_NAME);
    }

    public AjendaBooker(ConnectionFactory<T> dataSource, String topic, Clock clock, String schemaName) throws Exception {
        this.dataSource = dataSource;
        this.clock = clock;
        this.topic = topic;
        this.schemaName = schemaName;
        this.tableName = Common.getTableNameForTopic(topic);
        InitializationModel.initTableForTopic(dataSource, topic, schemaName);
    }

    public T getConnection() throws Exception {
        T connection = this.dataSource.getConnection();
        //if (connection.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
        return connection;
    }

    public void shutdown(long gracePeriod) {
        //TODO
    }

    public String getTopic() {
        return topic;
    }


    public String getTableNameWithSchema() {
        return '\"' + schemaName + "\"." + tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Clock getClock() {
        return clock;
    }

    public void bookAppointment(AppointmentBooking booking) throws Exception {
        BookModel.book(this.getTableNameWithSchema(), this, this.getClock(), booking, 0);
    }

}
