package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.murillo.ajenda.core.Common.extractAppointmentDue;

class AtLeastOnceModel {

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
                    + "  timeout_date = GREATEST(due_date, ?) + ?,"
                    + "  attempts = attempts + 1 "
                    + "WHERE uuid = any(array("
                    + "  SELECT uuid "
                    + "  FROM %s "
                    + "  WHERE due_date <= ? "
                    + "    AND timeout_date <= ? "
                    + "    AND %s "  //CUSTOM CONDITION
                    + "  ORDER BY due_date, timeout_date "
                    + "  FETCH FIRST ? ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *";

    private static final String ACK_ONE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid = ?";

    public static void process(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long nowEpoch,
            long timeout,
            boolean blocking,
            CancellableAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            scheduleAndAckIndividually(
                    ajendaScheduler,
                    nowEpoch,
                    lookAhead,
                    timeout,
                    blocking,
                    listener,
                    selectAndUpdate(ajendaScheduler, lookAhead, minSize, nowEpoch, timeout, customSqlCondition)
            );
        }
    }

    public static void process(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long nowEpoch,
            long timeout,
            boolean blocking,
            TransactionalAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            scheduleAndAckIndividually(
                    ajendaScheduler,
                    nowEpoch,
                    lookAhead,
                    timeout,
                    blocking,
                    listener,
                    selectAndUpdate(ajendaScheduler, lookAhead, minSize, nowEpoch, timeout, customSqlCondition)
            );
        }
    }

    private static void scheduleAndAckIndividually(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            long lookAhead,
            long timeout,
            boolean blocking,
            CancellableAppointmentListener listener,
            List<AppointmentDue> appointments) throws InterruptedException {
        Semaphore semaphore = blocking ?
                new Semaphore(0)
                : null;
        String tableName = ajendaScheduler.getTableNameWithSchema();
        int scheduled = 0;
        try {
            for (AppointmentDue a : appointments) {
                long delay = a.getDueTimestamp() - nowEpoch;
                CancelFlag cancelFlag = new CancelFlag();
                final ScheduledFuture<?> future = ajendaScheduler.getExecutor().schedule(() -> {
                            try {
                                try {
                                    if (isNotExpired(ajendaScheduler.getClock(), a)
                                            && isNotMissed(a, delay)) {
                                        ajendaScheduler.addBeganToProcess(a.getDueTimestamp());
                                        listener.receive(a, cancelFlag);
                                        ajendaScheduler.addProcessed(1);
                                    }
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
                                            a,
                                            lookAhead,
                                            cancelFlag);
                                } catch (Throwable th) {
                                    //TODO log
                                    th.printStackTrace();
                                    return;
                                }
                            } finally {
                                if (blocking) semaphore.release();
                            }
                        },
                        delay > 0 ? delay : 0,
                        TimeUnit.MILLISECONDS);
                scheduled++;
                ajendaScheduler.getPoller().schedule(
                        () -> {
                            cancelFlag.setCancelled(true);
                            future.cancel(true);
                        },
                        (delay > 0 ? delay : 0) + timeout,
                        TimeUnit.MILLISECONDS);
            }
        } finally {
            if (blocking) semaphore.acquire(scheduled);
        }
    }

    private static void scheduleAndAckIndividually(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            long lookAhead,
            long timeout,
            boolean blocking,
            TransactionalAppointmentListener listener,
            List<AppointmentDue> appointments) throws InterruptedException {

        Semaphore semaphore = blocking ?
                new Semaphore(0)
                : null;
        String tableName = ajendaScheduler.getTableNameWithSchema();
        int scheduled = 0;
        try {
            for (AppointmentDue a : appointments) {
                long delay = a.getDueTimestamp() - nowEpoch;
                CancelFlag cancelFlag = new CancelFlag();
                final ScheduledFuture<?> future = ajendaScheduler.getExecutor().schedule(() -> {

                            try {
                                if (!cancelFlag.isCancelled()) executeAndAck(
                                        ajendaScheduler,
                                        tableName,
                                        listener,
                                        a,
                                        lookAhead,
                                        cancelFlag,
                                        delay
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
                scheduled++;
                ajendaScheduler.getPoller().schedule(
                        () -> {
                            cancelFlag.setCancelled(true);
                            future.cancel(true);
                        },
                        (delay > 0 ? delay : 0) + timeout,
                        TimeUnit.MILLISECONDS);
            }
        } finally {
            if (blocking) semaphore.acquire(scheduled);
        }
    }    
    
    private static void ackIndividually(AjendaScheduler ajendaScheduler, String tableName, AppointmentDue appointmentDue, long lookAhead, CancelFlag cancelFlag) throws Exception {
        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(connection -> {
                performAckIndividually(
                        tableName,
                        appointmentDue,
                        connection,
                        ajendaScheduler.getTableNameWithSchema(),
                        ajendaScheduler.getPeriodicTableNameWithSchema(),
                        ajendaScheduler.getClock());
                return null;
            });
            if (!cancelFlag.isCancelled()) conn.commit();
        }
    }

    private static void performAckIndividually(
            String tableName,
            AppointmentDue appointmentDue,
            Connection conn,
            String tableNameWithSchema,
            String periodicTableNameWithSchema,
            Clock clock) throws SQLException {
        String sql = String.format(ACK_ONE_QUERY, tableName);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, appointmentDue.getAppointmentUid());
            stmt.execute();
            BookModel.bookNextIterations(
                    appointmentDue,
                    tableNameWithSchema,
                    periodicTableNameWithSchema,
                    conn,
                    clock.nowEpochMs());
        }
    }

    private static void executeAndAck(
            AjendaScheduler ajendaScheduler,
            String tableName,
            TransactionalAppointmentListener listener,
            AppointmentDue appointmentDue,
            long lookAhead,
            CancelFlag cancelFlag,
            long delay) throws Exception {
        AtomicBoolean toCommit = new AtomicBoolean(false);
        try (ConnectionWrapper connw = ajendaScheduler.getConnection()) {
            connw.doWork(connection -> {
                performExecuteAndAck(ajendaScheduler, tableName, listener, appointmentDue, cancelFlag, delay, connw, connection);
                return null;
            });
            if (toCommit.get()) connw.commit();
        }
    }

    private static void performExecuteAndAck(AjendaScheduler ajendaScheduler, String tableName, TransactionalAppointmentListener listener, AppointmentDue appointmentDue, CancelFlag cancelFlag, long delay, ConnectionWrapper connw, Connection connection) throws SQLException {
        String sql = String.format(ACK_ONE_QUERY, tableName);
        final long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, appointmentDue.getAppointmentUid());
            stmt.execute();
            BookModel.bookNextIterations(
                    appointmentDue,
                    ajendaScheduler.getTableNameWithSchema(),
                    ajendaScheduler.getPeriodicTableNameWithSchema(),
                    connection,
                    nowEpoch);
        }
        try {
            if (isNotExpired(ajendaScheduler.getClock(), appointmentDue)
                    && isNotMissed(appointmentDue, delay)) {
                ajendaScheduler.addBeganToProcess(appointmentDue.getDueTimestamp());
                listener.receive(appointmentDue, cancelFlag, new AbstractAjendaBooker(ajendaScheduler) {
                    @Override
                    public ConnectionWrapper getConnection() {
                        return connw;
                    }
                });
                if (!cancelFlag.isCancelled()) {

                    ajendaScheduler.addProcessed(1);
                }
            }
        } catch (UnhandledAppointmentException th) {
            //Controlled error
        } catch (Throwable th) {
            //TODO log
            //TODO free or ignore until expiration
            th.printStackTrace();
        }
    }

    private static List<AppointmentDue> selectAndUpdate(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableNameWithSchema();
        List<AppointmentDue> appointments = new ArrayList<>(limitSize);

        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(conection -> {
                performSelectAndUpdate(pollPeriod, limitSize, nowEpoch, timeout, customSqlCondition, tableName, appointments, conection);
                return null;
            });
            conn.commit();
            ajendaScheduler.addRead(appointments.size());
        }
        return appointments;
    }

    private static void performSelectAndUpdate(long pollPeriod, int limitSize, long nowEpoch, long timeout, String customSqlCondition, String tableName, List<AppointmentDue> appointments, Connection conn) throws SQLException {
        long limitDueDate = nowEpoch + pollPeriod;

        String sql = String.format(
                AT_LEAST_ONCE_ACKED_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition);


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

        }
    }

    private static boolean isNotMissed(AppointmentDue appointmentDue, long delay) {
        return delay >= 0
                || !AjendaFlags.isSkipMissed(appointmentDue.getFlags());
    }

    private static boolean isNotExpired(Clock clock, AppointmentDue appointmentDue) {
        return appointmentDue.getTtl() <= 0
                || (clock.nowEpochMs() - appointmentDue.getTtl()) <
                appointmentDue.getDueTimestamp();
    }

}
