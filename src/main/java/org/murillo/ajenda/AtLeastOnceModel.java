package org.murillo.ajenda;

import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.CancellableAppointmentListener;
import org.murillo.ajenda.dto.TransactionalAppointmentListener;
import org.murillo.ajenda.dto.UnhandledAppointmentException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.murillo.ajenda.Common.extractAppointmentDue;

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
                    + "  expiry_date = GREATEST(due_date, ?) + ?,"
                    + "  attempts = attempts + 1 "
                    + "WHERE uuid = any(array("
                    + "  SELECT uuid "
                    + "  FROM %s "
                    + "  WHERE due_date <= ? "
                    + "    AND expiry_date <= ? "
                    + "    AND %s "  //CUSTOM CONDITION
                    + "  ORDER BY due_date, expiry_date "
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
                    try{
                            try {
                                ajendaScheduler.addBeganToProcess(a.getDueTimestamp());
                                listener.receive(a, cancelFlag);
                                ajendaScheduler.addProcessed(1);
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
                                if(!cancelFlag.isCancelled()) executeAndAck(
                                        ajendaScheduler,
                                        tableName,
                                        listener,
                                        a,
                                        lookAhead,
                                        cancelFlag
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
        try (Connection conn = ajendaScheduler.getConnection()) {
            String sql = String.format(ACK_ONE_QUERY, tableName);
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setObject(1, appointmentDue.getAppointmentUid());
                stmt.execute();
                BookModel.bookNextIterations(
                        appointmentDue,
                        ajendaScheduler.getTableNameWithSchema(),
                        ajendaScheduler.getPeriodicTableNameWithSchema(),
                        conn,
                        ajendaScheduler.getClock().nowEpochMs());
                if(!cancelFlag.isCancelled()) conn.commit();
            }
        }
    }

    private static void executeAndAck(
            AjendaScheduler ajendaScheduler,
            String tableName,
            TransactionalAppointmentListener listener,
            AppointmentDue appointmentDue,
            long lookAhead,
            CancelFlag cancelFlag) throws Exception {
        try (Connection conn = ajendaScheduler.getConnection()) {
            String sql = String.format(ACK_ONE_QUERY, tableName);
            final long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setObject(1, appointmentDue.getAppointmentUid());
                stmt.execute();
                BookModel.bookNextIterations(
                        appointmentDue,
                        ajendaScheduler.getTableNameWithSchema(),
                        ajendaScheduler.getPeriodicTableNameWithSchema(),
                        conn,
                        nowEpoch);
            }
            try {
                ajendaScheduler.addBeganToProcess(appointmentDue.getDueTimestamp());
                listener.receive(appointmentDue, cancelFlag, new AbstractAjendaBooker(ajendaScheduler) {
                    @Override
                    public Connection getConnection() {
                        return conn;
                    }
                });
                if(!cancelFlag.isCancelled()) {
                    conn.commit();
                    ajendaScheduler.addProcessed(1);
                }
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

        String tableName = ajendaScheduler.getTableNameWithSchema();
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
                ajendaScheduler.addRead(appointments.size());
            }
        }
        return appointments;
    }

}
