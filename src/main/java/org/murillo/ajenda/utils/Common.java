package org.murillo.ajenda.utils;

import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class Common {
    public static String getTableNameForTopic(String topic) {
        return String.format("ajenda_%s", topic).toLowerCase(Locale.ENGLISH);
    }

    public static long nowEpoch() {
        return Instant.now().toEpochMilli();
    }

    public static boolean shutdown(
            ScheduledThreadPoolExecutor executor,
            long gracePeriodMs) {
        executor.shutdownNow();
        try {
            return executor.awaitTermination(gracePeriodMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //TODO
            e.printStackTrace();
            return false;
        }
    }

    public static ThreadFactory newDaemonExecutorThreadFactory() {
        ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        return r -> {
            Thread thread = defaultThreadFactory.newThread(r);
            thread.setDaemon(true);
            return thread;
        };
    }

}
