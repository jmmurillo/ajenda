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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AjendaScheduler<T extends Connection> implements ConnectionFactory<T> {

    public static final String DEFAULT_SCHEMA_NAME = "public";
    private final ConnectionFactory<T> dataSource;
    private final String topic;
    private final String schemaName;
    private final String tableName;
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
        InitializationModel.initTableForTopic(dataSource, topic, schemaName);
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

    public class CheckAgenda {

        private String customCondition;

        public CheckAgenda withCustomSqlCondition(String customSqlCondition) {
            this.customCondition = customSqlCondition;
            return this;
        }

        public CheckAgendaOnce once(int limitSize) {
            return new CheckAgendaOnce(limitSize, this.customCondition);
        }

        public CheckAgendaPeriodically periodically(int limitSize, long pollPeriodMs) {
            return new CheckAgendaPeriodically(limitSize, pollPeriodMs, this.customCondition);
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

        public void readAtLeastOnceAckEach(long timeout, SimpleAppointmentListener listener) throws Exception {
            AtLeastOnceAckEachModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    timeout,
                    true,
                    listener,
                    customCondition);
        }

        public void readAtLeastOnceAckJoin(long timeout, SimpleAppointmentListener listener) throws Exception {
            AtLeastOnceAckJoinModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    timeout,
                    true,
                    listener,
                    customCondition);
        }

        public void readAtLeastOnceAtomic(SimpleAppointmentListener listener) throws Exception {
            AtLeastOnceAtomicModel.process(
                    AjendaScheduler.this,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    listener,
                    customCondition);
        }

        //Connection In
        public void readAtLeastOnceAckEach(long timeout, ConnectionInAppointmentListener listener) throws Exception {
            AtLeastOnceAckEachModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    timeout,
                    true,
                    listener,
                    customCondition);
        }

        public void readAtLeastOnceAtomic(ConnectionInAppointmentListener listener) throws Exception {
            AtLeastOnceAtomicModel.process(
                    AjendaScheduler.this,
                    limitSize,
                    AjendaScheduler.this.clock.nowEpochMs(),
                    listener,
                    customCondition);
        }
    }

    public class CheckAgendaPeriodically {

        private int limitSize;
        private long pollPeriodMs;
        private String customCondition;
        //TODO Hacer posible parar y cambiar el periodo

        public CheckAgendaPeriodically(int limitSize, long pollPeriodMs, String customCondition) {
            if (limitSize < 1) throw new IllegalArgumentException("limitSize must be greater than zero");
            this.limitSize = limitSize;
            if (pollPeriodMs < 1) throw new IllegalArgumentException("pollPeriodMs must be greater than zero");
            this.pollPeriodMs = pollPeriodMs;
            this.customCondition = customCondition;
        }

        public void readAtMostOnce(boolean onlyLate, boolean reBookOnException, SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
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

        public void readAtLeastOnceAckEach(long timeout, SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                        try {
                            AtLeastOnceAckEachModel.process(
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

        public void readAtLeastOnceAckJoin(long timeout, boolean onlyLate, SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                        try {
                            AtLeastOnceAckJoinModel.process(
                                    AjendaScheduler.this,
                                    pollPeriodMs,
                                    limitSize,
                                    AjendaScheduler.this.clock.nowEpochMs(),
                                    timeout,
                                    onlyLate,
                                    listener,
                                    customCondition);
                        } catch (Exception e) {
                            //TODO
                            e.printStackTrace();
                        }
                    },
                    0,
                    pollPeriodMs,
                    TimeUnit.MILLISECONDS
            );
        }

        public void readAtLeastOnceAtomic(SimpleAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                        try {
                            AtLeastOnceAtomicModel.process(
                                    AjendaScheduler.this,
                                    limitSize,
                                    AjendaScheduler.this.clock.nowEpochMs(),
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

        //Connection In
        public void readAtLeastOnceAckEach(long timeout, ConnectionInAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                        try {
                            AtLeastOnceAckEachModel.process(
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
                    0,
                    pollPeriodMs,
                    TimeUnit.MILLISECONDS
            );
        }

        public void readAtLeastOnceAtomic(ConnectionInAppointmentListener listener) throws Exception {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(() -> {
                        try {
                            AtLeastOnceAtomicModel.process(
                                    AjendaScheduler.this,
                                    limitSize,
                                    AjendaScheduler.this.clock.nowEpochMs(),
                                    listener,
                                    customCondition);
                        } catch (Exception e) {
                            //TODO
                            e.printStackTrace();
                        }
                    },
                    0,
                    pollPeriodMs,
                    TimeUnit.MILLISECONDS
            );
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
