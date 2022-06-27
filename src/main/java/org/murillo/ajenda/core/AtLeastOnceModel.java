package org.murillo.ajenda.core;

import org.murillo.ajenda.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.murillo.ajenda.core.Common.extractAppointmentDue;

public class AtLeastOnceModel {

    private AtLeastOnceModel() {
    }

    private static final String UNEXPECTED_ERROR_PROCESSING_APPOINTMENT_STRF = "Unexpected error processing appointment %s";
    private static final Logger LOGGER = LoggerFactory.getLogger(AtLeastOnceModel.class);

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
                    + "  FETCH FIRST (?) ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *";

    private static final String ACK_ONE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid = ? "
                    + "AND version = ? ";

    private static final String MAY_BE_IN_PROGRESS =
            "SELECT MIN(timeout_date) "
                    + "  FROM %s "
                    + "  WHERE timeout_date >= ? "
                    + "  AND uuid = ? ";

    private static final String PERIODIC_MAY_BE_IN_PROGRESS =
            "SELECT MIN(timeout_date)"
                    + "  FROM %s"
                    + "  WHERE timeout_date >= ?"
                    + "  AND periodic_uuid = ?"
                    + " RETURNING *";

    static void process(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long timeout,
            boolean blocking,
            CancellableAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
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

    static void process(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long timeout,
            boolean blocking,
            TransactionalAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
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
            List<AppointmentDue> appointments
    ) throws InterruptedException {
        Semaphore semaphore = blocking ?
                new Semaphore(0)
                : null;
        String tableName = ajendaScheduler.getTableNameWithSchema();
        int scheduled = 0;
        try {
            for (AppointmentDue a : appointments) {
                long delay = a.getDueTimestamp() - nowEpoch;
                CancelFlag cancelFlag = new CancelFlag();
                final ScheduledFuture<?> future = ajendaScheduler.schedule(
                        a.getAppointmentUid(),
                        () -> {
                            try {
                                if (!cancelFlag.isCancelled())
                                    executeThenAck(
                                            ajendaScheduler,
                                            lookAhead,
                                            listener,
                                            tableName,
                                            a,
                                            delay,
                                            cancelFlag
                                    );

                            } catch (Throwable th) {
                                //TODO log
                                th.printStackTrace();
                                return;
                            } finally {
                                if (blocking) semaphore.release();
                            }
                        },
                        delay > 0 ? delay : 0,
                        TimeUnit.MILLISECONDS
                );
                scheduled++;
                ajendaScheduler.getPoller().schedule(
                        () -> {
                            cancelFlag.setCancelled(true);
                            future.cancel(true);
                        },
                        (delay > 0 ? delay : 0) + timeout,
                        TimeUnit.MILLISECONDS
                );
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
            List<AppointmentDue> appointments
    ) throws InterruptedException {

        Semaphore semaphore = blocking ?
                new Semaphore(0)
                : null;
        String tableName = ajendaScheduler.getTableNameWithSchema();
        int scheduled = 0;
        try {
            for (AppointmentDue a : appointments) {
                long delay = a.getDueTimestamp() - nowEpoch;
                CancelFlag cancelFlag = new CancelFlag();
                final ScheduledFuture<?> future = ajendaScheduler.schedule(
                        a.getAppointmentUid(),
                        () -> {
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
                                LOGGER.error(String.format(UNEXPECTED_ERROR_PROCESSING_APPOINTMENT_STRF, a), th);
                                //TODO free or ignore until expiration
                            } finally {
                                if (blocking) semaphore.release();
                            }
                        },
                        delay > 0 ? delay : 0,
                        TimeUnit.MILLISECONDS
                );
                scheduled++;
                ajendaScheduler.getPoller().schedule(
                        () -> {
                            cancelFlag.setCancelled(true);
                            future.cancel(true);
                        },
                        (delay > 0 ? delay : 0) + timeout,
                        TimeUnit.MILLISECONDS
                );
            }
        } finally {
            if (blocking) semaphore.acquire(scheduled);
        }
    }

    private static void ackIndividually(
            AjendaScheduler ajendaScheduler,
            String tableName,
            AppointmentDue appointmentDue,
            long lookAhead,
            CancelFlag cancelFlag
    ) throws Exception {
        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(connection -> {
                performAckIndividually(
                        tableName,
                        appointmentDue,
                        connection,
                        ajendaScheduler.getTableNameWithSchema(),
                        ajendaScheduler.getPeriodicTableNameWithSchema(),
                        ajendaScheduler.getClock()
                );
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
            Clock clock
    ) throws SQLException {
        String sql = String.format(ACK_ONE_QUERY, tableName);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setObject(1, appointmentDue.getAppointmentUid());
            stmt.setShort(2, appointmentDue.getVersion());
            stmt.execute();
            BookModel.bookNextIterations(
                    appointmentDue,
                    tableNameWithSchema,
                    periodicTableNameWithSchema,
                    conn,
                    clock.nowEpochMs()
            );
        }
    }

    private static void executeThenAck(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            CancellableAppointmentListener listener,
            String tableName,
            AppointmentDue appointmentDue,
            long delay,
            CancelFlag cancelFlag
    ) throws Exception {
        try {
            if (isNotExpired(ajendaScheduler.getClock(), appointmentDue)
                    && isNotMissed(appointmentDue, delay)) {
                ajendaScheduler.addBeganToProcess(appointmentDue.getDueTimestamp());
                LOGGER.debug("About to process appointment {}", appointmentDue);
                listener.receive(appointmentDue, cancelFlag);
                LOGGER.debug("Just processed appointment {}", appointmentDue);
                ajendaScheduler.addProcessed(1);
            } else {
                LOGGER.debug("Ignoring appointment because it is expired or missed: {}", appointmentDue);
            }

            ackIndividually(
                    ajendaScheduler,
                    tableName,
                    appointmentDue,
                    lookAhead,
                    cancelFlag
            );
        } catch (UnhandledAppointmentException th) {
            //Controlled exception
            LOGGER.warn(String.format("Controlled error processing appointment %s", appointmentDue), th);
        } catch (Throwable th) {
            //Unexpected exception
            LOGGER.error(String.format(UNEXPECTED_ERROR_PROCESSING_APPOINTMENT_STRF, appointmentDue), th);
        }

    }

    private static void executeAndAck(
            AjendaScheduler ajendaScheduler,
            String tableName,
            TransactionalAppointmentListener listener,
            AppointmentDue appointmentDue,
            long lookAhead,
            CancelFlag cancelFlag,
            long delay
    ) throws Exception {
        try (ConnectionWrapper connw = ajendaScheduler.getConnection()) {
            boolean toCommit = connw.doWork(connection -> performExecuteAndAck(
                    ajendaScheduler,
                    tableName,
                    listener,
                    appointmentDue,
                    cancelFlag,
                    delay,
                    connw,
                    connection
            ));
            if (toCommit && !cancelFlag.isCancelled()) {
                connw.commit();
                ajendaScheduler.addProcessed(1);
            }
        }
    }

    private static boolean performExecuteAndAck(
            AjendaScheduler ajendaScheduler,
            String tableName,
            TransactionalAppointmentListener listener,
            AppointmentDue appointmentDue,
            CancelFlag cancelFlag,
            long delay,
            ConnectionWrapper connw,
            Connection connection
    ) throws SQLException {
        String sql = String.format(ACK_ONE_QUERY, tableName);
        final long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setObject(1, appointmentDue.getAppointmentUid());
            stmt.setShort(2, appointmentDue.getVersion());
            stmt.execute();
            BookModel.bookNextIterations(
                    appointmentDue,
                    ajendaScheduler.getTableNameWithSchema(),
                    ajendaScheduler.getPeriodicTableNameWithSchema(),
                    connection,
                    nowEpoch
            );
        }
        try {
            if (isNotExpired(ajendaScheduler.getClock(), appointmentDue)
                    && isNotMissed(appointmentDue, delay)) {
                ajendaScheduler.addBeganToProcess(appointmentDue.getDueTimestamp());
                LOGGER.debug("About to process appointment {}", appointmentDue);
                listener.receive(appointmentDue, cancelFlag, new AbstractAjendaBooker(ajendaScheduler) {
                    @Override
                    public ConnectionWrapper getConnection() {
                        return connw;
                    }

                    @Override
                    protected void cancelInQueue(List<UUID> uuids, Map<UUID, CancelledResult> cancelledResultMap) {
                        //Do nothing
                    }

                    @Override
                    protected void periodicCancelInQueue(
                            List<UUID> periodicUuids,
                            Map<UUID, CancelledResult> cancelledResultMap
                    ) {
                        //Do nothing
                    }
                });
                LOGGER.debug("Just processed appointment {}", appointmentDue);
                return true;
            } else {
                LOGGER.debug("Ignoring appointment because it is expired or missed: {}", appointmentDue);
            }
        } catch (UnhandledAppointmentException th) {
            //Controlled error
            LOGGER.warn(String.format("Controlled error processing appointment %s", appointmentDue), th);
        } catch (Throwable th) {
            LOGGER.error(String.format(UNEXPECTED_ERROR_PROCESSING_APPOINTMENT_STRF, appointmentDue), th);
            //TODO free or ignore until expiration
        }
        return false;
    }

    private static List<AppointmentDue> selectAndUpdate(
            AjendaScheduler ajendaScheduler,
            long lookAhead,
            int limitSize,
            long nowEpoch,
            long timeout,
            String customSqlCondition
    ) throws Exception {

        String tableName = ajendaScheduler.getTableNameWithSchema();
        List<AppointmentDue> appointments = new ArrayList<>(limitSize);

        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(conection -> {
                performSelectAndUpdate(
                        lookAhead,
                        limitSize,
                        nowEpoch,
                        timeout,
                        customSqlCondition,
                        tableName,
                        appointments,
                        conection
                );
                return null;
            });
            conn.commit();
            ajendaScheduler.addRead(appointments.size());
        }
        return appointments;
    }

    private static void performSelectAndUpdate(
            long lookAhead,
            int limitSize,
            long nowEpoch,
            long timeout,
            String customSqlCondition,
            String tableName,
            List<AppointmentDue> appointments,
            Connection conn
    ) throws SQLException {

        long limitDueDate = nowEpoch + lookAhead;

        String sql = String.format(
                AT_LEAST_ONCE_ACKED_QUERY,
                tableName,
                tableName,
                customSqlCondition == null || customSqlCondition.isEmpty() ? "TRUE"
                        : customSqlCondition
        );


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
            LOGGER.debug("Polled with query {}. Retrieved {} appointments", stmt, appointments.size());
        }
    }

    public static Long mayBeInProgressUntil(AjendaScheduler ajendaScheduler, UUID appointmentUuid) throws Exception {
        String tableName = ajendaScheduler.getTableNameWithSchema();
        String sql = String.format(
                MAY_BE_IN_PROGRESS,
                tableName
        );

        long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
        Long[] resultPointer = new Long[]{null};
        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(connection -> {
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setLong(1, nowEpoch);
                    stmt.setObject(2, appointmentUuid);
                    ResultSet resultSet = stmt.executeQuery();
                    if (resultSet.next()) {
                        resultPointer[0] = resultSet.getLong(1);
                        if (resultSet.wasNull()) resultPointer[0] = null;
                    }
                    LOGGER.debug(
                            "Appointment with uuid {} {} in progress in AtLeastOnceModel{}.",
                            appointmentUuid,
                            (resultPointer[0] != null ? "MAY BE" : "IS NOT"),
                            (resultPointer[0] != null ? " until " + resultPointer[0] : "")
                    );
                }
                return null;
            });
        }

        return resultPointer[0];
    }

    public static Long periodicIterationMayBeInProgressUntil(
            AjendaScheduler ajendaScheduler,
            UUID periodicAppointmentUuid
    ) throws Exception {

        String tableName = ajendaScheduler.getTableNameWithSchema();
        String sql = String.format(
                PERIODIC_MAY_BE_IN_PROGRESS,
                tableName
        );

        long nowEpoch = ajendaScheduler.getClock().nowEpochMs();
        Long[] resultPointer = new Long[]{null};
        try (ConnectionWrapper conn = ajendaScheduler.getConnection()) {
            conn.doWork(connection -> {
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setLong(1, nowEpoch);
                    stmt.setObject(2, periodicAppointmentUuid);
                    ResultSet resultSet = stmt.executeQuery();
                    if (resultSet.next()) {
                        resultPointer[0] = resultSet.getLong(1);
                        if (resultSet.wasNull()) resultPointer[0] = null;
                    }
                    LOGGER.debug(
                            "An iteration of the periodic appointment with periodic uuid {} {} in progress in AtLeastOnceModel{}.",
                            periodicAppointmentUuid,
                            (resultPointer[0] != null ? "MAY BE" : "IS NOT"),
                            (resultPointer[0] != null ? " until " + resultPointer[0] : "")
                    );
                }
                return null;
            });
        }

        return resultPointer[0];
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
