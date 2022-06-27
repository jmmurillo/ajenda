package org.murillo.ajenda.core;

import com.google.common.collect.MapMaker;
import org.murillo.ajenda.dto.CancellableAppointmentListener;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.SimpleAppointmentListener;
import org.murillo.ajenda.dto.TransactionalAppointmentListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AjendaScheduler extends AbstractAjendaBooker {

    public static final int CONNECTION_VALIDATION_TIMEOUT_SEC = 3;
    private static final String ABOUT_TO_POLL_TOPIC_LOGF = "About to poll topic {}";
    private static final String UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF = "Unexpected error when polling topic %s";
    private static final String THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF = "Thread was interrupted while polling topic %s";
    private static final Logger LOGGER = LoggerFactory.getLogger(AjendaScheduler.class);

    public static final String DEFAULT_SCHEMA_NAME = "public";
    private static final int DEFAULT_QUEUE_SIZE = 1000;

    protected int maxQueueSize;
    protected ScheduledThreadPoolExecutor executor;
    protected ScheduledThreadPoolExecutor poller;
    protected volatile ScheduledFuture<?> pollerScheduledFuture = null;
    private boolean ownClock = false;

    private long creationTime;
    private AtomicLong readCount = new AtomicLong(0);
    private AtomicLong processedCount = new AtomicLong(0);
    private long beganToProcessCount = 0;
    private double meanLag = 0.0;

    private Map<UUID, ScheduledFuture<?>> scheduledFutureCache;
    private boolean mayInterruptOnCancel = true;

    public AjendaScheduler(ConnectionFactory dataSource, String topic, String customSchema) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), customSchema);
        this.ownClock = true;
    }

    public AjendaScheduler(ConnectionFactory dataSource, String topic) throws Exception {
        this(dataSource, topic, new SyncedClock(dataSource), DEFAULT_SCHEMA_NAME);
        this.ownClock = true;
    }

    public AjendaScheduler(
            ConnectionFactory dataSource,
            String topic,
            Clock clock,
            String customSchema
    ) throws Exception {
        this(
                dataSource,
                topic,
                Runtime.getRuntime().availableProcessors(),
                DEFAULT_QUEUE_SIZE,
                clock,
                customSchema
        );
    }

    public AjendaScheduler(ConnectionFactory dataSource, String topic, Clock clock) throws Exception {
        this(
                dataSource,
                topic,
                Runtime.getRuntime().availableProcessors(),
                DEFAULT_QUEUE_SIZE,
                clock,
                DEFAULT_SCHEMA_NAME
        );
    }

    public AjendaScheduler(
            ConnectionFactory dataSource,
            String topic,
            int concurrencyLevel,
            int maxQueueSize
    ) throws Exception {
        this(dataSource, topic, concurrencyLevel, maxQueueSize, new SyncedClock(dataSource), DEFAULT_SCHEMA_NAME);
        this.ownClock = true;
    }

    public AjendaScheduler(
            ConnectionFactory dataSource,
            String topic,
            int concurrencyLevel,
            int maxQueueSize,
            Clock clock
    ) throws Exception {
        this(dataSource, topic, concurrencyLevel, maxQueueSize, clock, DEFAULT_SCHEMA_NAME);
    }

    public AjendaScheduler(
            ConnectionFactory dataSource,
            String topic,
            int concurrencyLevel,
            int maxQueueSize,
            Clock clock,
            String schemaName
    ) throws Exception {
        super(
                dataSource,
                topic,
                schemaName,
                clock
        );

        if (concurrencyLevel <= 0) throw new IllegalArgumentException("concurrencyLevel must be greater than zero");
        if (maxQueueSize <= 0) throw new IllegalArgumentException("maxQueueSize must be greater than zero");

        InitializationModel.initTableForTopic(dataSource, topic, schemaName, tableName, periodicTableName);
        this.maxQueueSize = maxQueueSize;
        this.scheduledFutureCache = new MapMaker().weakValues().makeMap();
        this.executor = new ScheduledThreadPoolExecutor(
                concurrencyLevel,
                new AjendaExecutorThreadFactory(topic),
                new ThreadPoolExecutor.DiscardPolicy()
        );
        this.executor.setRemoveOnCancelPolicy(true);
        this.poller = new ScheduledThreadPoolExecutor(
                1,
                new AjendaPollerThreadFactory(topic),
                new ThreadPoolExecutor.DiscardPolicy()
        );
        this.poller.setRemoveOnCancelPolicy(true);
        this.creationTime = clock.nowEpochMs();

        //TODO Ofrecer estadísticas de trabajos en proceso, en cola, etc.
    }

    public boolean isMayInterruptOnCancel() {
        return mayInterruptOnCancel;
    }

    public void setMayInterruptOnCancel(boolean mayInterruptOnCancel) {
        this.mayInterruptOnCancel = mayInterruptOnCancel;
    }

    public ScheduledFuture<?> schedule(
            UUID appointmentUid,
            Runnable runnable,
            long delay,
            TimeUnit unit
    ) {
        ScheduledFuture<?> scheduledFuture = this.executor.schedule(runnable, delay, unit);
        this.scheduledFutureCache.put(appointmentUid, scheduledFuture);
        return scheduledFuture;
    }

    public Long tryCancelInQueue(UUID appointmentUUID, boolean mayInterrupt) {
        ScheduledFuture<?> toCancel = this.scheduledFutureCache.get(appointmentUUID);
        if (toCancel != null) {
            long delayNs = toCancel.getDelay(TimeUnit.NANOSECONDS);
            if (!(toCancel.isDone() || (delayNs <= 0 && !mayInterrupt) || !toCancel.cancel(mayInterrupt))) {
                return TimeUnit.MILLISECONDS.convert(delayNs, TimeUnit.NANOSECONDS);
            }
        }
        return null;
    }

    private static class AjendaPollerThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        AjendaPollerThreadFactory(String topic) {
            SecurityManager s = System.getSecurityManager();
            this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = "ajenda-" + topic + "-poller-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            t.setDaemon(true);
            t.setPriority((Thread.NORM_PRIORITY + Thread.MIN_PRIORITY) / 2);

            return t;
        }
    }

    private static class AjendaExecutorThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        AjendaExecutorThreadFactory(String topic) {
            SecurityManager s = System.getSecurityManager();
            this.group = s != null ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = "ajenda-" + topic + "-executor-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(), 0L);
            t.setDaemon(false);
            t.setPriority(Thread.NORM_PRIORITY);

            return t;
        }
    }

    public boolean shutdown(long gracePeriodMs) throws InterruptedException {
        this.poller.shutdownNow();
        this.executor.shutdownNow();
        if (ownClock) this.clock.shutdown(0L);
        return this.executor.awaitTermination(gracePeriodMs, TimeUnit.MILLISECONDS);
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

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public ConnectionWrapper getConnection() throws Exception {
        ConnectionWrapper connection = this.dataSource.getConnection();
        return connection;
    }

    public ScheduledThreadPoolExecutor getExecutor() {
        return executor;
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    protected void cancelInQueue(List<UUID> uuids, Map<UUID, CancelledResult> cancelledResultMap) {
        uuids.stream().forEach(uuid -> {
            long nowEpoch = this.clock.nowEpochMs();
            Long delay = this.tryCancelInQueue(uuid, this.mayInterruptOnCancel);
            if (delay != null) {
                cancelledResultMap.putIfAbsent(uuid, new CancelledResult(uuid, null, nowEpoch + delay, -1));
            }
        });
    }

    @Override
    protected void periodicCancelInQueue(List<UUID> periodicUuids, Map<UUID, CancelledResult> cancelledResultMap) {
        //TODO There may be iterations in queue which won't be cancelled.
        // Only iterations present in main table will be cancelled.
        cancelledResultMap.forEach((key, value) -> {
            UUID uuid = value.getUuid();
            this.tryCancelInQueue(uuid, this.mayInterruptOnCancel);
        });
    }

    public CheckAgenda checkAgenda() {
        return new CheckAgenda();
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

    public double getMeanLag() {
        return meanLag;
    }

    public synchronized void addBeganToProcess(long dueTimestamp) {
        beganToProcessCount++;
        final long lag = this.clock.nowEpochMs() - dueTimestamp;
        meanLag = (meanLag * beganToProcessCount + lag) / ++beganToProcessCount;
    }

    public void stopPolling() {
        this.pollerScheduledFuture.cancel(false);
        this.pollerScheduledFuture = null;
    }

    public boolean isPolling() {
        return this.pollerScheduledFuture != null &&
                this.poller.getTaskCount() > 0 &&
                !(this.pollerScheduledFuture.isCancelled() || this.pollerScheduledFuture.isDone());
    }

    public boolean checkConnection() {
        try (ConnectionWrapper connection = this.getConnection()) {
            return connection.doWork(
                    c -> c.isValid(CONNECTION_VALIDATION_TIMEOUT_SEC)
            );
        } catch (Exception e) {
            LOGGER.error("Exception occurred checking connection", e);
            return false;
        }
    }

    public boolean checkPoller() {
        if (poller.isShutdown()) return false;
        if (pollerScheduledFuture == null) return true;
        if (poller.getQueue().size() + poller.getActiveCount() <= 0) return false;
        return !this.pollerScheduledFuture.isCancelled() && !this.pollerScheduledFuture.isDone();
    }

    public class CheckAgenda {

        private String customCondition;
        private int fetchSize = AjendaScheduler.this.maxQueueSize;

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
                    true,
                    reBookOnException,
                    true,
                    listener,
                    customCondition
            );
        }

        public void readAtLeastOnce(long timeout, CancellableAppointmentListener listener) throws Exception {
            AtLeastOnceModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    timeout,
                    true,
                    listener,
                    customCondition
            );
        }

        //Connection In
        public void readAtLeastOnceTransactional(
                long timeout,
                TransactionalAppointmentListener listener
        ) throws Exception {
            AtLeastOnceModel.process(
                    AjendaScheduler.this,
                    0,
                    limitSize,
                    timeout,
                    true,
                    listener,
                    customCondition
            );
        }
    }

    public class CheckAgendaPeriodically {

        private int limitSize;
        private long pollPeriodMs;
        private long periodDeviationMs;
        private String customCondition;
        //TODO Hacer posible parar y cambiar el periodo

        public CheckAgendaPeriodically(
                int limitSize,
                long pollPeriodMs,
                long periodDeviationMs,
                String customCondition
        ) {
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

        public void readAtMostOnce(
                boolean onlyLate,
                boolean reBookOnException,
                SimpleAppointmentListener listener
        ) {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleWithFixedDelay(
                        () -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                LOGGER.debug(ABOUT_TO_POLL_TOPIC_LOGF, AjendaScheduler.this.getTopic());
                                AtMostOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        onlyLate,
                                        reBookOnException,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (Exception th) {
                                //TODO
                                //Show must go on
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(
                        () -> {
                            try {
                                //LOGGER.debug("About to poll topic {}", AjendaScheduler.this.getTopic());
                                AtMostOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        onlyLate,
                                        reBookOnException,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (InterruptedException e) {
                                LOGGER.error(String.format(
                                        THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), e);
                                Thread.currentThread().interrupt();
                            } catch (Exception th) {
                                //TODO
                                //Show must go on
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        public void readAtLeastOnce(long timeout, CancellableAppointmentListener listener) {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.poller.scheduleWithFixedDelay(
                        () -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                LOGGER.debug(ABOUT_TO_POLL_TOPIC_LOGF, AjendaScheduler.this.getTopic());
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        timeout,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (InterruptedException e) {
                                LOGGER.error(String.format(
                                        THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), e);
                                Thread.currentThread().interrupt();
                            } catch (Exception th) {
                                //TODO
                                //Show must go on
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.poller.scheduleAtFixedRate(
                        () -> {
                            try {
                                LOGGER.debug(ABOUT_TO_POLL_TOPIC_LOGF, AjendaScheduler.this.getTopic());
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        timeout,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (InterruptedException e) {
                                LOGGER.error(String.format(
                                        THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), e);
                                Thread.currentThread().interrupt();
                            } catch (Exception th) {
                                //TODO
                                //Show must go on
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

        //Connection In
        public void readAtLeastOnceTransactional(
                long timeout,
                TransactionalAppointmentListener listener
        ) {
            long remainingDelay = 0L;
            if (AjendaScheduler.this.pollerScheduledFuture != null) {
                remainingDelay = AjendaScheduler.this.pollerScheduledFuture.getDelay(TimeUnit.MILLISECONDS);
                AjendaScheduler.this.pollerScheduledFuture.cancel(false);
            }
            if (this.periodDeviationMs > 0) {
                long minimumPeriod = pollPeriodMs - periodDeviationMs;
                AtomicLong currentSleep = new AtomicLong(0);
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleWithFixedDelay(
                        () -> {
                            try {
                                long nextSleep = ThreadLocalRandom.current()
                                        .nextLong(0, 2 * periodDeviationMs);
                                Thread.sleep(currentSleep.getAndSet(nextSleep));
                                LOGGER.debug(ABOUT_TO_POLL_TOPIC_LOGF, AjendaScheduler.this.getTopic());
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        minimumPeriod + nextSleep,
                                        limitSize,
                                        timeout,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (InterruptedException e) {
                                LOGGER.error(String.format(
                                        THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), e);
                                Thread.currentThread().interrupt();
                            } catch (Exception th) {
                                //TODO
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        minimumPeriod,
                        TimeUnit.MILLISECONDS
                );
            } else {
                AjendaScheduler.this.pollerScheduledFuture = AjendaScheduler.this.poller.scheduleAtFixedRate(
                        () -> {
                            try {
                                LOGGER.debug(ABOUT_TO_POLL_TOPIC_LOGF, AjendaScheduler.this.getTopic());
                                AtLeastOnceModel.process(
                                        AjendaScheduler.this,
                                        pollPeriodMs,
                                        limitSize,
                                        timeout,
                                        false,
                                        listener,
                                        customCondition
                                );
                            } catch (InterruptedException e) {
                                LOGGER.error(String.format(
                                        THREAD_WAS_INTERRUPTED_WHILE_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), e);
                                Thread.currentThread().interrupt();
                            } catch (Exception th) {
                                //TODO
                                LOGGER.error(String.format(
                                        UNEXPECTED_ERROR_WHEN_POLLING_TOPIC_STRF,
                                        AjendaScheduler.this.getTopic()
                                ), th);
                            }
                        },
                        remainingDelay,
                        pollPeriodMs,
                        TimeUnit.MILLISECONDS
                );
            }
        }

    }

    void addRead(int read) {
        this.readCount.addAndGet(read);
    }

    void addProcessed(int processed) {
        this.processedCount.addAndGet(processed);
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

    ScheduledThreadPoolExecutor getPoller() {
        return poller;
    }

    public long getCreationTime() {
        return creationTime;
    }
}
