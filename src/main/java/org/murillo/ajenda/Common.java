package org.murillo.ajenda;

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
        long creation_date = 0;
        long due_date = 0;
        long expiry_date = 0;
        int attempts = 0;
        String payload = null;
        UUID periodicAppointmentUid = null;
        HashMap<String, Object> extraParams = new HashMap<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            switch (metaData.getColumnName(i)) {
                case "uuid":
                    uuid = UUID.fromString(rs.getString(i));
                    break;
                case "creation_date":
                    creation_date = rs.getLong(i);
                    break;
                case "due_date":
                    due_date = rs.getLong(i);
                    break;
                case "expiry_date":
                    expiry_date = rs.getLong(i);
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
                default:
                    extraParams.put(metaData.getColumnName(i), rs.getObject(i));
                    break;
            }
        }

        return new AppointmentDue(
                uuid,
                due_date,
                nowEpoch,
                payload,
                attempts,
                extraParams.isEmpty() ? null : extraParams,
                periodicAppointmentUid
        );
    }

    public static PeriodicAppointmentBooking extractPeriodicAppointment(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        UUID uuid = null;
        long creation_date = 0;
        PeriodicPatternType pattern_type = null;
        String pattern = null;
        String payload = null;
        boolean skip_missed = true;
        HashMap<String, Object> extraParams = new HashMap<>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            switch (metaData.getColumnName(i)) {
                case "uuid":
                    uuid = UUID.fromString(rs.getString(i));
                    break;
                case "creation_date":
                    creation_date = rs.getLong(i);
                    break;
                case "pattern_type":
                    pattern_type = PeriodicPatternType.fromId(rs.getInt(i));
                    break;
                case "pattern":
                    pattern = rs.getString(i);
                    break;
                case "payload":
                    payload = rs.getString(i);
                    break;
                case "skip_missed":
                    skip_missed = rs.getBoolean(i);
                    break;
                default:
                    extraParams.put(metaData.getColumnName(i), rs.getObject(i));
                    break;
            }
        }

        return new PeriodicAppointmentBooking(
                uuid,
                pattern_type,
                pattern,
                payload,
                extraParams.isEmpty() ? null : extraParams,
                skip_missed,
                -1L
        );
    }

}
