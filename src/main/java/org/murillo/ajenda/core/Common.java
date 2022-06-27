package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.PeriodicAppointmentBooking;
import org.murillo.ajenda.dto.PeriodicPatternType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class Common {

    private Common() {
    }

    public static String getTableNameForTopic(String topic) {
        return String.format("ajenda_%s", topic).toLowerCase(Locale.ENGLISH);
    }

    public static String getPeriodicTableNameForTopic(String topic) {
        return String.format("periodic_ajenda_%s", topic).toLowerCase(Locale.ENGLISH);
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

    public static AppointmentDue extractAppointmentDue(ResultSet rs, long nowEpoch) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        UUID uuid = null;
        long creationDate = 0;
        long dueDate = 0;
        int ttl = 0;
        long timeoutDate = 0;
        int attempts = 0;
        String payload = null;
        UUID periodicAppointmentUid = null;
        int flags = 0;
        short version = 0;
        HashMap<String, Object> extraParams = new HashMap<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            switch (metaData.getColumnName(i)) {
                case "uuid":
                    uuid = UUID.fromString(rs.getString(i));
                    break;
                case "creation_date":
                    creationDate = rs.getLong(i);
                    break;
                case "due_date":
                    dueDate = rs.getLong(i);
                    break;
                case "ttl":
                    ttl = rs.getInt(i);
                    break;
                case "timeout_date":
                    timeoutDate = rs.getLong(i);
                    break;
                case "attempts":
                    attempts = rs.getInt(i);
                    break;
                case "payload":
                    payload = rs.getString(i);
                    break;
                case "periodic_uuid":
                    String optional = rs.getString(i);
                    periodicAppointmentUid = optional != null ?
                            UUID.fromString(rs.getString(i))
                            : null;
                    break;
                case "flags":
                    flags = rs.getInt(i);
                    break;
                case "version":
                    version = rs.getShort(i);
                    break;
                default:
                    extraParams.put(metaData.getColumnName(i), rs.getObject(i));
                    break;
            }
        }

        return new AppointmentDue(
                uuid,
                dueDate,
                ttl,
                payload,
                attempts,
                extraParams.isEmpty() ? null : extraParams,
                periodicAppointmentUid,
                flags,
                version
        );
    }

    public static PeriodicAppointmentBooking extractPeriodicAppointment(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        UUID uuid = null;
        long creationDate = 0;
        PeriodicPatternType patternType = null;
        String pattern = null;
        int ttl = 0;
        String payload = null;
        int keyIteration = 1;
        boolean skipMissed = true;
        HashMap<String, Object> extraParams = new HashMap<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            switch (metaData.getColumnName(i)) {
                case "uuid":
                    uuid = UUID.fromString(rs.getString(i));
                    break;
                case "creation_date":
                    creationDate = rs.getLong(i);
                    break;
                case "pattern_type":
                    patternType = PeriodicPatternType.fromId(rs.getInt(i));
                    break;
                case "pattern":
                    pattern = rs.getString(i);
                    break;
                case "ttl":
                    ttl = rs.getInt(i);
                    break;
                case "payload":
                    payload = rs.getString(i);
                    break;
                case "key_iteration":
                    keyIteration = rs.getInt(i);
                    break;
                case "skip_missed":
                    skipMissed = rs.getBoolean(i);
                    break;
                default:
                    extraParams.put(metaData.getColumnName(i), rs.getObject(i));
                    break;
            }
        }

        return new PeriodicAppointmentBooking(
                uuid,
                patternType,
                pattern,
                ttl,
                payload,
                extraParams.isEmpty() ? null : extraParams,
                keyIteration,
                skipMissed,
                -1L,
                false,
                0
        );
    }

}
