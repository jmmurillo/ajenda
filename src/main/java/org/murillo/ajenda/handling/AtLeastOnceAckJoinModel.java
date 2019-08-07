package org.murillo.ajenda.handling;

import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.SimpleAppointmentListener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.murillo.ajenda.handling.Utils.extractAppointmentDue;

public class AtLeastOnceAckJoinModel {

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
                    + "  ORDER BY due_date, expiry_date ASC "
                    + "  FETCH FIRST ? ROWS ONLY "
                    + "  FOR UPDATE SKIP LOCKED "
                    + ")) RETURNING *;";

    private static final String ACK_MULTIPLE_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid IN (%s);";

    public static void process(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            boolean onlyLate,
            SimpleAppointmentListener listener,
            String customSqlCondition
    ) throws Exception {
        JoinAck joinAck;
        synchronized (ajendaScheduler.getExecutor()) {
            int minSize = Math.min(limitSize, ajendaScheduler.remainingSlots());
            if (minSize <= 0) return;
            joinAck = scheduleAndJoinForAck(
                    ajendaScheduler,
                    nowEpoch,
                    listener,
                    selectAndUpdate(ajendaScheduler, pollPeriod, minSize, nowEpoch, timeout, onlyLate, customSqlCondition)
            );
        }
        //Join timed tasks
        try {
            joinAck.join();
        } catch (Exception e) {
            //TODO Log
            //TODO free or ignore until expiration
            e.printStackTrace();
        }
    }


    private static JoinAck scheduleAndJoinForAck(
            AjendaScheduler ajendaScheduler,
            long nowEpoch,
            SimpleAppointmentListener listener,
            List<AppointmentDue> appointments) {

        String tableName = ajendaScheduler.getTableName();
        JoinAck joinAck = new JoinAck(ajendaScheduler, appointments.size());
        try {
            for (AppointmentDue a : appointments) {
                long delay = a.getDueTimestamp() - nowEpoch;
                ajendaScheduler.getExecutor().schedule(() -> {
                            try {
                                listener.receive(a);
                                joinAck.notifySuccess(a.getAppointmentUid());
                            } catch (Throwable th) {
                                //TODO log
                                //TODO free or ignore until expiration
                                th.printStackTrace();
                                return;
                            } finally {
                                joinAck.notifyEnd();
                            }
                        },
                        delay > 0 ? delay : 0,
                        TimeUnit.MILLISECONDS);
                joinAck.notifyScheduled();
            }
        } catch (Throwable th) {
            //TODO
        } finally {
            return joinAck;
        }
    }

    private static class JoinAck {

        AjendaScheduler ajendaScheduler;
        private Set<UUID> successful;
        private Semaphore semaphore;
        private int scheduledSize;

        public JoinAck(AjendaScheduler ajendaScheduler, int appointmentsSize) {
            this.ajendaScheduler = ajendaScheduler;
            successful = ConcurrentHashMap.newKeySet(appointmentsSize);
            semaphore = new Semaphore(0);
            scheduledSize = 0;
        }

        public void notifyScheduled() {
            scheduledSize++;
        }

        public void notifySuccess(UUID appointmentUid) {
            successful.add(appointmentUid);
        }

        public void notifyEnd() {
            semaphore.release();
        }

        public void join() throws Exception {
            try {
                semaphore.acquire(scheduledSize);
            } finally {
                ackMultiple(ajendaScheduler, successful);
            }
        }
    }

    private static void ackMultiple(AjendaScheduler ajendaScheduler, Set<UUID> successful) throws Exception {
        //Make a copy set to avoid concurrent modification in case of interruption
        Set<UUID> toAck = new HashSet<>(successful);
        if (!toAck.isEmpty()) {
            try (Connection conn = ajendaScheduler.getConnection()) {
                String questionMarks = String.join(",",
                        toAck.stream().map(x -> "?").collect(Collectors.toList()));
                String sql = String.format(ACK_MULTIPLE_QUERY, ajendaScheduler.getTableName(), questionMarks);
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    int i = 1;
                    for (UUID uid : toAck) {
                        stmt.setObject(i++, uid);
                    }
                    stmt.execute();
                    conn.commit();
                }
            }
        }
    }

    private static List<AppointmentDue> selectAndUpdate(
            AjendaScheduler ajendaScheduler,
            long pollPeriod,
            int limitSize,
            long nowEpoch,
            long timeout,
            boolean onlyLate,
            String customSqlCondition) throws Exception {

        String tableName = ajendaScheduler.getTableName();
        long limitDueDate = onlyLate ? nowEpoch : nowEpoch + pollPeriod;

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
