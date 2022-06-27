package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.AppointmentBookingBuilder;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.SimpleAppointmentListener;
import org.murillo.ajenda.dto.UnhandledAppointmentException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.murillo.ajenda.core.Common.extractAppointmentDue;

public class AtMostOnceModel {

    //AT_MOST_ONCE_QUERY (DELETE)
    //COMMIT
    //PROCESS
    //:limitDueDate, :now, :size
    static AtomicInteger atomicInteger = new AtomicInteger(0);

    private static final String AT_MOST_ONCE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid = any(array("
                    + "  SELECT uuid "
                    + "  FROM %s "
                    + "  WHERE due_date <= ? "
                    + "    AND timeout_date <= ? "
                    + "  ORDER BY due_date, timeout_date ASC "
                    + "  FETCH FIRST (?) ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *";

    static void process(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            boolean onlyLate,
            boolean reBookOnException,
            boolean blocking,
            SimpleAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
            scheduleNoAck(
                    ajendaScheduler,
                    lookAhead,
                    nowEpoch,
                    onlyLate,
                    reBookOnException,
                    blocking,
                    listener,
                    selectAndDelete(ajendaScheduler, lookAhead, minSize, nowEpoch, onlyLate, customSqlCondition)
            );
        }
    }

    private static void scheduleNoAck(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            long nowEpoch,
            boolean onlyLate,
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
                    ajendaScheduler.schedule(
                            appointmentDue.getAppointmentUid(),
                            () -> {
                                try {
                                    if (isNotExpired(ajendaScheduler, appointmentDue)
                                            && isNotMissed(lookAhead, onlyLate, appointmentDue, delay)) {
                                        ajendaScheduler.addBeganToProcess(appointmentDue.getDueTimestamp());
                                        listener.receive(appointmentDue);
                                        ajendaScheduler.addProcessed(1);
                                    }
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

    private static boolean isNotMissed(long lookAhead, boolean onlyLate, AppointmentDue appointmentDue, long delay) {
        return delay >= (onlyLate ? -lookAhead : 0)
                || !AjendaFlags.isSkipMissed(appointmentDue.getFlags());
    }

    private static boolean isNotExpired(AjendaScheduler ajendaScheduler, AppointmentDue appointmentDue) {
        return appointmentDue.getTtl() <= 0
                || (ajendaScheduler.getClock().nowEpochMs() - appointmentDue.getTtl()) <
                appointmentDue.getDueTimestamp();
    }

    private static void reBook(AjendaScheduler ajendaScheduler, AppointmentDue appointmentDue) {
        AppointmentBookingBuilder builder = AppointmentBookingBuilder.aBooking()
                .withUid(appointmentDue.getAppointmentUid())
                .withDueTimestamp(appointmentDue.getDueTimestamp())
                .withPayload(appointmentDue.getPayload());
        if (appointmentDue.getExtraParams() != null) {
            appointmentDue.getExtraParams().forEach(builder::withExtraParam);
        }
        //TODO keep periodic_uid or not ?
        try {
            BookModel.book(
                    ajendaScheduler.getTableNameWithSchema(),
                    ajendaScheduler,
                    ajendaScheduler.getClock(),
                    appointmentDue.getAttempts() + 1,
                    Arrays.asList(builder.build())
            );
        } catch (Throwable th1) {
            //TODO log
            th1.printStackTrace();
        }
    }

    private static synchronized List<AppointmentDue> selectAndDelete(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long nowEpoch,
            boolean onlyLate,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableNameWithSchema();
        long limitDueDate = onlyLate ? nowEpoch : nowEpoch + lookAhead;

        String sql = String.format(
                AT_MOST_ONCE_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition);

        List<AppointmentDue> appointments = new ArrayList<>(limitSize);


        try (ConnectionWrapper connw = ajendaScheduler.getConnection()) {
            connw.doWork(connection -> {
                if (connection.getAutoCommit())
                    throw new IllegalStateException("Connection must have auto-commit disabled");
                //:limitDueDate, :now, :size
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setLong(1, limitDueDate);
                    stmt.setLong(2, nowEpoch);
                    stmt.setInt(3, limitSize);
                    ResultSet resultSet = stmt.executeQuery();
                    while (resultSet.next()) {
                        AppointmentDue appointmentDue = extractAppointmentDue(resultSet, nowEpoch);
                        BookModel.bookNextIterations(
                                appointmentDue,
                                ajendaScheduler.getTableNameWithSchema(),
                                ajendaScheduler.getPeriodicTableNameWithSchema(),
                                connection,
                                nowEpoch);
                        appointments.add(appointmentDue);
                    }

                }
                connw.commit();
                ajendaScheduler.addRead(appointments.size());
                return null;
            });
        }
        return appointments;
    }
}
