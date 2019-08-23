package org.murillo.ajenda.handling;

import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.common.ConnectionFactory;
import org.murillo.ajenda.common.InitializationModel;
import org.murillo.ajenda.common.SyncedClock;
import org.murillo.ajenda.dto.ConnectionInAppointmentListener;
import org.murillo.ajenda.dto.SimpleAppointmentListener;
import org.murillo.ajenda.utils.Common;

import java.sql.Connection;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.murillo.ajenda.utils.Common.getPeriodicTableNameForTopic;

public class AjendaScheduler<T extends Connection> implements ConnectionFactory<T> {

    public static final String DEFAULT_SCHEMA_NAME = "public";
    private final ConnectionFactory<T> dataSource;
    private final String topic;
    private final String schemaName;
    private final String tableName;
    private final String periodicTableName;
    private final int maxQueueSize;
    private final ScheduledThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor poller;
    private final Clock clock;
    protected volatile ScheduledFuture<?> pollerScheduledFuture = null;
    private boolean ownClock = false;
    private AjendaBooker derivedBooker;

    public AjendaScheduler(ConnectionFactory<T> dataSource, String topic, String customSchema) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), customSchema);
        this.ownClock = true;
    }

    public AjendaScheduler(ConnectionFactory<T> dataSource, String topic) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), DEFAULT_SCHEMA_NAME);
        this.ownClock = true;
    }

    public AjendaScheduler(ConnectionFactory<T> dataSource, String topic, Clock clock, String customSchema) throws Exception {
        this(
                dataSource,
                topic,
                Runtime.getRuntime().availableProcessors(),
                3 * Runtime.getRuntime().availableProcessors(),
                clock,
                customSchema);
    }

    public AjendaScheduler(ConnectionFactory<T> dataSource, String topic, Clock clock) throws Exception {
        this(
                dataSource,
                topic,
                Runtime.getRuntime().availableProcessors(),
                3 * Runtime.getRuntime().availableProcessors(),
                clock,
                DEFAULT_SCHEMA_NAME);
    }

    public AjendaScheduler(ConnectionFactory<T> dataSource, String topic, int concurrencyLevel, int maxQueueSize) throws Exception {
        this(dataSource, topic, concurrencyLevel, maxQueueSize, new SyncedClock(dataSource), DEFAULT_SCHEMA_NAME);
        this.ownClock = true;
    }

    public AjendaScheduler(
            ConnectionFactory<T> dataSource,
            String topic,
            int concurrencyLevel,
            int maxQueueSize,
            Clock clock
    ) throws Exception {
        this(dataSource, topic, concurrencyLevel, maxQueueSize, clock, DEFAULT_SCHEMA_NAME);
    }

    public AjendaScheduler(
            ConnectionFactory<T> dataSource,
            String topic,
            int concurrencyLevel,
            int maxQueueSize,
            Clock clock,
            String schemaName
    ) throws Exception {
        if (dataSource == null) throw new IllegalArgumentException("dataSource must not be null");
        if (clock == null) throw new IllegalArgumentException("clock must not be null");
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic must not be empty");
        if (schemaName == null || schemaName.isEmpty())
            throw new IllegalArgumentException("schema name must not be empty");
        if (concurrencyLevel <= 0) throw new IllegalArgumentException("concurrencyLevel must be greater than zero");
        if (maxQueueSize <= 0) throw new IllegalArgumentException("maxQueueSize must be greater than zero");

        this.dataSource = dataSource;
        this.clock = clock;
        this.topic = topic;
        this.schemaName = schemaName;
        this.tableName = Common.getTableNameForTopic(topic);
        this.periodicTableName = getPeriodicTableNameForTopic(topic);
        InitializationModel.initTableForTopic(dataSource, topic, schemaName, tableName, periodicTableName);
        this.maxQueueSize = maxQueueSize;
        this.executor = new ScheduledThreadPoolExecutor(concurrencyLevel, new ThreadPoolExecutor.DiscardPolicy());

        this.poller = new ScheduledThreadPoolExecutor(1, new ThreadPoolExecutor.DiscardPolicy());
        this.derivedBooker = new AjendaBooker(this) {
            @Override
            public void shutdown(long gracePeriod) {
            }
        };
        //TODO Ofrecer estadísticas de trabajos en proceso, en cola, etc.
    }

    public boolean shutdown(long gracePeriodMs) {
        this.poller.shutdownNow();
        this.executor.shutdownNow();
        if (ownClock) this.clock.shutdown(0L);
        try {
            return this.executor.awaitTermination(gracePeriodMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getTableNameWithSchema() {
        return '\"' + schemaName + "\"." + tableName;
    }

    public String getPeriodicTableNameWithSchema() {
        return '\"' + schemaName + "\"." + periodicTableName;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public T getConnection() throws Exception {
        T connection = this.dataSource.getConnection();
        if (connection.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
        return connection;
    }

    public ScheduledThreadPoolExecutor getExecutor() {
        return executor;
    }

    public AjendaBooker derivedBooker() {
        return this.derivedBooker;
    }

    public Clock getClock() {
        return clock;
    }

    public CheckAgenda checkAgenda() {
        return new CheckAgenda();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPeriodicTableName() {
        return periodicTableName;
    }

    public class CheckAgenda {

        private String customCondition;
        private int fetchSize = 1;

        public CheckAgenda withCustomSqlCondition(String customSqlCondition) {
            this.customCondition = customSqlCondition;
            return this;
        }

        public CheckAgenda withFetchSize(int fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public CheckAgendaOnce once() {
            return new CheckAgendaOnce(fetchSize, this.customCondition);
        }

        public CheckAgendaPeriodically periodically(long pollPeriodMs) {
            return new CheckAgendaPeriodically(fetchSize, pollPeriodMs, 0, this.customCondition);
        }

        public CheckAgendaPeriodically periodically(long meanPollPeriodMs, long periodDeviationMs) {
            return new CheckAgendaPeriodically(fetchSize, meanPollPeriodMs, periodDeviationMs, this.customCondition);
        }

    }

    public class CheckAgendaOnce {

        private int limitSize;
        private String customCondition;

        public CheckAgendaOnce(int limitSize, String customCondition) {
            this.limitSize = limitSize;
            this.customCondition = customCondition;
        }

        public void readAtMostOnce(boolean reBookOnException, SimpleAppointmentListener listener) throws Exception {
            AtMostOnceModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    true,
                    reBookOnException,
                    true,
                    listener,
                    customCondition);
        }

        public void readAtLeastOnce(long timeout, SimpleAppointmentListener listener) throws Exception {
            AtLeastOnceModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    timeout,
                    true,
                    listener,
                    customCondition);
        }

        //Connection In
        public void readAtLeastOnce(long timeout, ConnectionInAppointmentListener listener) throws Exception {
            AtLeastOnceModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    timeout,
                    true,
                    listener,
                    customCondition);
        }
    }

    public class CheckAgendaPeriodically {

        private int limitSize;
        private long pollPeriodMs;
        private long periodDeviationMs;
        private String customCondition;
        //TODO Hacer posible parar y cambiar el periodo

        public CheckAgendaPeriodically(int limitSize, long pollPeriodMs, long periodDeviationMs, String customCondition) {
            if (limitSize < 1) throw new IllegalArgumentException("fetchSize must be greater than zero");
            this.limitSize = limitSize;
            if (pollPeriodMs < 1) throw new IllegalArgumentException("pollPeriodMs must be greater than zero");
            if (periodDeviationMs < 0) throw new IllegalArgumentException("periodDeviationMs must not be negative");
            if (periodDeviationMs > pollPeriodMs)
                throw new IllegalArgumentException("periodDeviationMs must not be greater than pollPeriodMs");

            this.pollPeriodMs = pollPeriodMs;
            this.periodDeviationMs = periodDeviationMs;
            this.customCondition = customCondition;
        }

        public void readAtMostOnce(boolean onlyLate, boolean reBookOnException, SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleWithFixedDelay(() -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                AtMostOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        onlyLate,
                                        reBookOnException,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Throwable th) {
                                //TODO
                                //Show must go on
                                th.printStackTrace();
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                            try {
                                AtMostOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        onlyLate,
                                        reBookOnException,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Throwable th) {
                                //TODO
                                //Show must go on
                                th.printStackTrace();
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        public void readAtLeastOnce(long timeout, SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.poller.scheduleWithFixedDelay(() -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        timeout,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Throwable th) {
                                //TODO
                                //Show must go on
                                th.printStackTrace();
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                            try {
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        timeout,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Throwable th) {
                                //TODO
                                //Show must go on
                                th.printStackTrace();
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        //Connection In
        public void readAtLeastOnce(long timeout, ConnectionInAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleWithFixedDelay(() -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        timeout,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Exception e) {
                                //TODO
                                e.printStackTrace();
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                            try {
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        AjendaScheduler.this.clock.nowEpochMs(),
                                        timeout,
                                        false,
                                        listener,
                                        customCondition);
                            } catch (Exception e) {
                                //TODO
                                e.printStackTrace();
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

    }

    public int remainingSlots() {
        return idleThreads() + queueFreeSlots();
    }

    public int idleThreads() {
        return this.executor.getCorePoolSize() - this.executor.getActiveCount();
    }

    public int queueFreeSlots() {
        return this.maxQueueSize - this.executor.getQueue().size();
    }

}
