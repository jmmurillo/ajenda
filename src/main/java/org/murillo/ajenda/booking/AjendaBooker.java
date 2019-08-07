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

    private final ConnectionFactory<T> dataSource;
    private final String topic;
    private final String tableName;
    private final Clock clock;

    public AjendaBooker(ConnectionFactory<T> dataSource, String topic) throws Exception {
        this(dataSource, new SyncedClock(dataSource), topic);
    }

    public AjendaBooker(AjendaScheduler<T> ajendaScheduler) {
        this.dataSource = ajendaScheduler;
        this.clock = ajendaScheduler.getClock();
        this.topic = ajendaScheduler.getTopic();
        this.tableName = Common.getTableNameForTopic(topic);
    }

    public void shutdown(long gracePeriod) {
        //TODO
    }

    public AjendaBooker(ConnectionFactory<T> dataSource, Clock clock, String topic) throws Exception {
        this.dataSource = dataSource;
        this.clock = clock;
        this.topic = topic;
        this.tableName = Common.getTableNameForTopic(topic);
        InitializationModel.initTableForTopic(dataSource, topic);
    }

    public T getConnection() throws Exception {
        T connection = this.dataSource.getConnection();
        //if (connection.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
        return connection;
    }

    public String getTopic() {
        return topic;
    }

    public String getTableName() {
        return tableName;
    }

    public Clock getClock() {
        return clock;
    }

    public void bookAppointment(AppointmentBooking booking) throws Exception {
        BookModel.book(this.getTableName(), this, this.getClock(), booking, 0);
    }
    
}
