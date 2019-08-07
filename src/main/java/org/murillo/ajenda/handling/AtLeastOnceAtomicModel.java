package org.murillo.ajenda.handling;

import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.ConnectionInAppointmentListener;
import org.murillo.ajenda.dto.SimpleAppointmentListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import static org.murillo.ajenda.handling.Utils.extractAppointmentDue;

public class AtLeastOnceAtomicModel {

    //AT_MOST_ONCE_QUERY (DELETE)
    //COMMIT
    //PROCESS
    //:limitDueDate, :now, :size
    private static final String AT_MOST_ONCE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid = any(array("
                    + "  SELECT uuid "
                    + "  FROM %s "
                    + "  WHERE due_date < ? "
                    + "    AND expiry_date < ? "
                    + "    AND %s "  //CUSTOM CONDITION
                    + "  ORDER BY due_date, expiry_date ASC "
                    + "  FETCH FIRST ? ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *;";

    public static void process(
            AjendaScheduler ajendaScheduler,
            int limitSize,
            long nowEpoch,
            SimpleAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
        if (minSize <= 0) return;
        selectScheduleAndDelete(ajendaScheduler, minSize, nowEpoch, listener, customSqlCondition);
    }

    public static void process(
            AjendaScheduler ajendaScheduler,
            int limitSize,
            long nowEpoch,
            ConnectionInAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
        if (minSize <= 0) return;
        selectScheduleAndDelete(ajendaScheduler, minSize, nowEpoch, listener, customSqlCondition);
    }

    private static void selectScheduleAndDelete(
            AjendaScheduler ajendaScheduler,
            int limitSize,
            long nowEpoch,
            SimpleAppointmentListener listener,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableName();
        //Take only late tasks, so execution should wait less
        long limitDueDate = nowEpoch;

        String sql = String.format(
                AT_MOST_ONCE_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition);

        List<AppointmentDue> appointments = new ArrayList<>(limitSize);

        try (Connection conn = ajendaScheduler.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            //:limitDueDate, :now, :size
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, limitDueDate);
                stmt.setLong(2, nowEpoch);
                stmt.setInt(3, limitSize);
                Semaphore semaphore = new Semaphore(0);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    AppointmentDue appointmentDue = extractAppointmentDue(resultSet, nowEpoch);
                    appointmentDue.setAttempts(appointmentDue.getAttempts() - 1);
                    appointments.add(appointmentDue);
                    try {
                        ajendaScheduler.getExecutor()
                                .execute(() -> {
                                    try {
                                        listener.receive(appointmentDue);
                                    } catch (Throwable th) {
                                        //TODO log
                                        //TODO call handler in another thread
                                        th.printStackTrace();
                                        return;
                                    } finally {
                                        semaphore.release();
                                    }
                                });
                    } catch (RejectedExecutionException ex) {
                        //TODO log
                        //TODO call handler in another thread
                        ex.printStackTrace();
                    }
                }
                //Join tasks
                semaphore.acquire(appointments.size());
                conn.commit();
            }
        }
    }

    private static void selectScheduleAndDelete(
            AjendaScheduler ajendaScheduler,
            int limitSize,
            long nowEpoch,
            ConnectionInAppointmentListener listener,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableName();
        //Take only late tasks, so execution should wait less
        long limitDueDate = nowEpoch;

        String sql = String.format(
                AT_MOST_ONCE_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition);

        List<AppointmentDue> appointments = new ArrayList<>(limitSize);

        try (Connection conn = ajendaScheduler.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            //:limitDueDate, :now, :size
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, limitDueDate);
                stmt.setLong(2, nowEpoch);
                stmt.setInt(3, limitSize);
                Semaphore semaphore = new Semaphore(0);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    AppointmentDue appointmentDue = extractAppointmentDue(resultSet, nowEpoch);
                    appointmentDue.setAttempts(appointmentDue.getAttempts() - 1);
                    appointments.add(appointmentDue);
                    try {
                        ajendaScheduler.getExecutor()
                                .execute(() -> {
                                    try {
                                        listener.receive(appointmentDue, new AjendaBooker(ajendaScheduler) {
                                            @Override
                                            public Connection getConnection() {
                                                return conn;
                                            }
                                        });
                                    } catch (Throwable th) {
                                        //TODO log
                                        //TODO call handler in another thread
                                        th.printStackTrace();
                                        return;
                                    } finally {
                                        semaphore.release();
                                    }
                                });
                    } catch (RejectedExecutionException ex) {
                        //TODO log
                        //TODO call handler in another thread
                        ex.printStackTrace();
                    }
                }
                //Join tasks
                semaphore.acquire(appointments.size());
                conn.commit();
            }
        }

    }
}
