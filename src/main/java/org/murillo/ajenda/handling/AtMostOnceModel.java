package org.murillo.ajenda.handling;

import org.murillo.ajenda.booking.BookModel;
import org.murillo.ajenda.dto.AppointmentBookingBuilder;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.SimpleAppointmentListener;
import org.murillo.ajenda.dto.UnhandledAppointmentException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.murillo.ajenda.handling.Utils.extractAppointmentDue;

public class AtMostOnceModel {

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
                    + "  WHERE due_date <= ? "
                    + "    AND expiry_date <= ? "
                    + "    AND %s "  //CUSTOM CONDITION
                    + "  ORDER BY due_date, expiry_date ASC "
                    + "  FETCH FIRST ? ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *";

    public static void process(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            boolean onlyLate,
            boolean reBookOnException,
            boolean blocking,
            SimpleAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            scheduleNoAck(
                    ajendaScheduler,
                    nowEpoch,
                    reBookOnException,
                    blocking,
                    listener,
                    selectAndDelete(ajendaScheduler, pollPeriod, minSize, nowEpoch, onlyLate, customSqlCondition)
            );
        }
    }

    private static void scheduleNoAck(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            boolean reBookOnException,
            boolean blocking,
            SimpleAppointmentListener listener,
            List<AppointmentDue> appointments) throws InterruptedException {
        Semaphore semaphore = blocking ?
                new Semaphore(0)
                : null;
        int scheduled = 0;
        try {
            for (AppointmentDue appointmentDue : appointments) {
                long delay = appointmentDue.getDueTimestamp() - nowEpoch;
                try {
                    ajendaScheduler.getExecutor()
                            .schedule(() -> {
                                        try {
                                            listener.receive(appointmentDue);
                                        } catch (UnhandledAppointmentException ex) {
                                            if (reBookOnException) {
                                                reBook(ajendaScheduler, appointmentDue);
                                            }
                                            //TODO log
                                            return;
                                        } catch (Throwable th) {
                                            th.printStackTrace();
                                            if (reBookOnException) {
                                                reBook(ajendaScheduler, appointmentDue);
                                            }
                                            //TODO log
                                            return;
                                        } finally {
                                            if (blocking) semaphore.release();
                                        }
                                    },
                                    delay > 0 ? delay : 0,
                                    TimeUnit.MILLISECONDS);
                    scheduled++;
                } catch (RejectedExecutionException ex) {
                    //TODO log
                    //TODO call handler in another thread
                    ex.printStackTrace();
                }
            }
        } finally {
            if (blocking) semaphore.acquire(scheduled);
        }
    }

    private static void reBook(AjendaScheduler ajendaScheduler, AppointmentDue appointmentDue) {
        AppointmentBookingBuilder builder = AppointmentBookingBuilder.aBooking()
                .withUid(appointmentDue.getAppointmentUid())
                .withDueTimestamp(appointmentDue.getDueTimestamp())
                .withPayload(appointmentDue.getPayload());
        if (appointmentDue.getExtraParams() != null) {
            appointmentDue.getExtraParams().forEach((k, v) -> {
                builder.withExtraParam(k, v);
            });
        }
        try {
            BookModel.rebook(
                    ajendaScheduler.getTableNameWithSchema(),
                    ajendaScheduler,
                    ajendaScheduler.getClock(),
                    builder.build(),
                    appointmentDue.getAttempts() + 1,
                    appointmentDue.getPeriodicAppointmentUid()
            );
        } catch (Throwable th1) {
            //TODO log
            th1.printStackTrace();
        }
    }

    private static List<AppointmentDue> selectAndDelete(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            boolean onlyLate,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableNameWithSchema();
        long limitDueDate = onlyLate ? nowEpoch : nowEpoch + pollPeriod;

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
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    appointments.add(extractAppointmentDue(resultSet, nowEpoch));
                }
                conn.commit();
            }
        }

        return appointments;
    }
}
