package org.murillo.ajenda.utils;

import org.murillo.ajenda.Common;
import org.murillo.ajenda.dto.Clock;
import org.murillo.ajenda.dto.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SyncedClock implements Clock {

    public static final int SYNC_ITERATIONS = 5;
    public static final int DEFAULT_SYNC_PERIOD_MINUTES = 15;
    private static String GET_TIME_QUERY = "SELECT EXTRACT(EPOCH FROM clock_timestamp()) * 1000";
    private volatile long offset = 0;
    private ConnectionFactory connectionFactory;
    private ScheduledThreadPoolExecutor executor;
    private ScheduledFuture<?> scheduledFuture;

    public SyncedClock(ConnectionFactory connectionFactory) {
        this(connectionFactory, DEFAULT_SYNC_PERIOD_MINUTES);
    }

    public SyncedClock(ConnectionFactory connectionFactory, int syncPeriodMinutes) {
        this.connectionFactory = connectionFactory;
        this.sync();
        this.executor = new ScheduledThreadPoolExecutor(1);
        this.executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduledFuture = executor.scheduleAtFixedRate(
                this::sync,
                syncPeriodMinutes,
                syncPeriodMinutes,
                TimeUnit.MINUTES);
    }

    public boolean shutdown(long gracePeriodMs){
        this.scheduledFuture.cancel(true);
        return Common.shutdown(this.executor, gracePeriodMs);
    }

    private void sync() {
        try {
            ArrayList<long[]> offsets = new ArrayList<>(SYNC_ITERATIONS);
            try (Connection conn = connectionFactory.getConnection()) {
                try (PreparedStatement preparedStatement = conn.prepareStatement(GET_TIME_QUERY)) {
                    long javaEpoch = System.currentTimeMillis();
                    long delay = System.nanoTime();
                    ResultSet resultSet = preparedStatement.executeQuery();
                    delay = (System.nanoTime() - delay) / 2000000L;
                    resultSet.next();
                    long dbEpoch = resultSet.getLong(1) - delay;
                    offsets.add(new long[]{dbEpoch - javaEpoch, delay});
                }
            }
            for (int i = 1; i < SYNC_ITERATIONS; i++) {
                Thread.sleep(1000);
                try (Connection conn = connectionFactory.getConnection()) {
                    try (PreparedStatement preparedStatement = conn.prepareStatement(GET_TIME_QUERY)) {
                        long javaEpoch = System.currentTimeMillis();
                        long delay = System.nanoTime();
                        if(Thread.currentThread().isInterrupted()) {
                            return;//Interrupted
                        }else{
                            ResultSet resultSet = preparedStatement.executeQuery();
                            delay = (System.nanoTime() - delay) / 2000000L;
                            resultSet.next();
                            long dbEpoch = resultSet.getLong(1) - delay;
                            offsets.add(new long[]{dbEpoch - javaEpoch, delay});
                        }
                    }
                }
            }
            this.offset = (long) offsets.stream()
                    .sorted(Comparator.comparingLong(a -> a[1]))
                    .mapToLong(a -> a[0]).limit(2)
                    .average()
                    .orElse(0.0);
        }catch (Exception ex){
            //TODO
        }
        System.out.println("OFFSET = " + offset);
    }

    public long nowEpochMs(){
        return System.currentTimeMillis() + this.offset;
    }

    public long getOffset() {
        return this.offset;
    }

}
