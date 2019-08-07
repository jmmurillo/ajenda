package org.murillo.ajenda.handling;

import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.ConnectionInAppointmentListener;
import org.murillo.ajenda.dto.SimpleAppointmentListener;
import org.murillo.ajenda.dto.UnhandledAppointmentException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.murillo.ajenda.handling.Utils.extractAppointmentDue;

public class AtLeastOnceAckEachModel {

    //AT_LEAST_ONCE_ACKED_QUERY
    //COMMIT
    //
    //PROCESS
    //
    //ACK_QUERY
    //COMMIT
    //:now, :timeout, :limitDueDate, :now, :size
    private static final String AT_LEAST_ONCE_ACKED_QUERY =
            "UPDATE %s "
                    + "SET "
                    + "  expiry_date = GREATEST(due_date, ?) + ?,"
                    + "  attempts = attempts + 1 "
                    + "WHERE uuid = any(array("
                    + "  SELECT uuid "
                    + "  FROM %s "
                    + "  WHERE due_date < ? "
                    + "    AND expiry_date < ? "
                    + "    AND %s "  //CUSTOM CONDITION
                    + "  ORDER BY due_date, expiry_date "
                    + "  FETCH FIRST ? ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *;";

    private static final String ACK_ONE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid = ?;";

    public static void process(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            SimpleAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            scheduleAndAckIndividually(
                    ajendaScheduler,
                    nowEpoch,
                    listener,
                    selectAndUpdate(ajendaScheduler, pollPeriod, minSize, nowEpoch, timeout, customSqlCondition)
            );
        }
    }

    public static void process(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            ConnectionInAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            scheduleAndAckIndividually(
                    ajendaScheduler,
                    nowEpoch,
                    listener,
                    selectAndUpdate(ajendaScheduler, pollPeriod, minSize, nowEpoch, timeout, customSqlCondition)
            );
        }
    }


    private static void scheduleAndAckIndividually(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            SimpleAppointmentListener listener,
            List<AppointmentDue> appointments) {

        String tableName = ajendaScheduler.getTableName();
        for (AppointmentDue a : appointments) {
            long delay = a.getDueTimestamp() - nowEpoch;
            ajendaScheduler.getExecutor().schedule(() -> {
                        try {
                            listener.receive(a);
                        } catch (UnhandledAppointmentException th) {
                            //Controlled exception
                            return;
                        } catch (Throwable th) {
                            //Unexpected exception
                            //TODO log
                            th.printStackTrace();
                            return;
                        }
                        try {
                            ackIndividually(
                                    ajendaScheduler,
                                    tableName,
                                    a.getAppointmentUid());
                        } catch (Throwable th) {
                            //TODO log
                            th.printStackTrace();
                            return;
                        }
                    },
                    delay > 0 ? delay : 0,
                    TimeUnit.MILLISECONDS);
        }
    }

    private static void scheduleAndAckIndividually(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            ConnectionInAppointmentListener listener,
            List<AppointmentDue> appointments) {

        String tableName = ajendaScheduler.getTableName();
        for (AppointmentDue a : appointments) {
            long delay = a.getDueTimestamp() - nowEpoch;
            ajendaScheduler.getExecutor().schedule(() -> {
                        try {
                            executeAndAck(
                                    ajendaScheduler,
                                    tableName,
                                    listener,
                                    a
                            );
                        } catch (Throwable th) {
                            //TODO log
                            //TODO free or ignore until expiration
                            th.printStackTrace();
                            return;
                        }
                    },
                    delay > 0 ? delay : 0,
                    TimeUnit.MILLISECONDS);
        }
    }

    private static void ackIndividually(AjendaScheduler ajendaScheduler, String tableName, UUID uid) throws Exception {
        try (Connection conn = ajendaScheduler.getConnection()) {
            String sql = String.format(ACK_ONE_QUERY, tableName);
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setObject(1, uid);
                stmt.execute();
                conn.commit();
            }
        }
    }

    private static void executeAndAck(
            AjendaScheduler ajendaScheduler,
            String tableName,
            ConnectionInAppointmentListener listener,
            AppointmentDue appointmentDue) throws Exception {
        try (Connection conn = ajendaScheduler.getConnection()) {
            String sql = String.format(ACK_ONE_QUERY, tableName);
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setObject(1, appointmentDue.getAppointmentUid());
                stmt.execute();
            }
            try {
                listener.receive(appointmentDue, new AjendaBooker(ajendaScheduler) {
                    @Override
                    public Connection getConnection() {
                        return conn;
                    }
                });
                conn.commit();
            } catch (UnhandledAppointmentException th) {
                //Controlled error
                return;
            } catch (Throwable th) {
                //TODO log
                //TODO free or ignore until expiration
                th.printStackTrace();
                return;
            }
        }
    }

    private static List<AppointmentDue> selectAndUpdate(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableName();
        long limitDueDate = nowEpoch + pollPeriod;

        String sql = String.format(
                AT_LEAST_ONCE_ACKED_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition);

        List<AppointmentDue> appointments = new ArrayList<>(limitSize);

        try (Connection conn = ajendaScheduler.getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, nowEpoch);
                stmt.setLong(2, timeout);
                stmt.setLong(3, limitDueDate);
                stmt.setLong(4, nowEpoch);
                stmt.setInt(5, limitSize);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    AppointmentDue appointmentDue = extractAppointmentDue(resultSet, nowEpoch);
                    appointmentDue.setAttempts(appointmentDue.getAttempts() - 1);
                    appointments.add(appointmentDue);
                }
                conn.commit();
            }
        }
        return appointments;
    }

}
