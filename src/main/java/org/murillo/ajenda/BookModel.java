package org.murillo.ajenda;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.murillo.ajenda.dto.*;
import org.murillo.ajenda.utils.UUIDType5;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.murillo.ajenda.Common.extractPeriodicAppointment;

class BookModel<T extends Connection> {

    private static final String BOOK_INSERT_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, due_date, timeout_date, ttl, attempts, payload, periodic_uuid, flags%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT (uuid) DO UPDATE SET "
                    + "(uuid, creation_date, due_date, timeout_date, ttl, attempts, payload, periodic_uuid, flags%s) "
                    + "= (?, ?, ?, ?, ?, ?, ?, ?, ?%s) ";

    private static final String BOOK_INSERT_DONT_UPDATE_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, due_date, timeout_date, ttl, attempts, payload, periodic_uuid, flags%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT (uuid) DO NOTHING";

    private static final String PERIODIC_BOOK_INSERT_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, pattern_type, pattern, ttl, payload, key_iteration, skip_missed%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT DO NOTHING ";

    private static final String PERIODIC_BOOK_SELECT_QUERY =
            "SELECT * "
                    + "FROM %s "
                    + "WHERE uuid = ? ";

    private static final String CANCEL_BOOKINGS_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid IN (%s) "
                    + "AND periodic_uuid IS NULL";

    private static final String CANCEL_PERIODIC_BOOKINGS_FROM_MAIN_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE periodic_uuid IN (%s)";

    private static final String CANCEL_PERIODIC_BOOKINGS_FROM_PERIODIC_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid IN (%s)";

    static void cancel(
            String tableName,
            ConnectionFactory<?> connectionFactory,
            List<UUID> uuids
    ) throws Exception {

        while (uuids.remove(null)) ;
        if (uuids.isEmpty()) return;

        String cancelSql = String.format(
                CANCEL_BOOKINGS_QUERY,
                tableName,
                buildQuestionMarks(uuids.size())
        );

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            try (PreparedStatement stmt = conn.prepareStatement(cancelSql)) {
                int place = 1;
                for (int i = 0; i < uuids.size(); i++) {
                    stmt.setObject(place++, uuids.get(i));
                }
                stmt.execute();
            }
            conn.commit();
        }
    }

    static void cancelPeriodic(
            String tableName,
            String periodicTableName,
            ConnectionFactory<?> connectionFactory,
            List<UUID> periodic_uuids
    ) throws Exception {

        while (periodic_uuids.remove(null)) ;
        if (periodic_uuids.isEmpty()) return;

        String questionMarks = buildQuestionMarks(periodic_uuids.size());
        String cancelFromPeriodicSql = String.format(
                CANCEL_PERIODIC_BOOKINGS_FROM_PERIODIC_QUERY,
                periodicTableName,
                questionMarks
        );

        String cancelFromMainSql = String.format(
                CANCEL_PERIODIC_BOOKINGS_FROM_MAIN_QUERY,
                tableName,
                questionMarks
        );

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            try (PreparedStatement stmt = conn.prepareStatement(cancelFromPeriodicSql)) {
                int place = 1;
                for (int i = 0; i < periodic_uuids.size(); i++) {
                    stmt.setObject(place++, periodic_uuids.get(i));
                }
                stmt.execute();
            }
            try (PreparedStatement stmt = conn.prepareStatement(cancelFromMainSql)) {
                int place = 1;
                for (int i = 0; i < periodic_uuids.size(); i++) {
                    stmt.setObject(place++, periodic_uuids.get(i));
                }
                stmt.execute();
            }
            conn.commit();
        }
    }

    static void book(
            String tableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            int previousAttempts,
            List<AppointmentBooking> bookings
    ) throws Exception {

        long nowEpochMs = clock.nowEpochMs();

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");

            for (AppointmentBooking booking : bookings) {

                ArrayList<Map.Entry<String, ?>> extraColumnsList;
                if (booking.getExtraParams() != null) {
                    extraColumnsList = new ArrayList<>(booking.getExtraParams().entrySet());
                } else {
                    extraColumnsList = null;
                }
                String extraColumnsNames = buildExtraColumnsNames(extraColumnsList);
                String extraColumnsQuestionMarks = buildExtraColumnsQuestionMarks(extraColumnsList);
                String bookSql = String.format(
                        BOOK_INSERT_QUERY,
                        tableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );

                try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {
                    int place = 1;

                    //FOR INSERT
                    stmt.setObject(place++, booking.getAppointmentUid());
                    stmt.setLong(place++, nowEpochMs);
                    long dueTimestamp = booking.getDueTimestamp() > 0 ?
                            booking.getDueTimestamp()
                            //If negative, time is a relative delay
                            : nowEpochMs - booking.getDueTimestamp();
                    stmt.setLong(place++, dueTimestamp);
                    stmt.setLong(place++, -1L);
                    stmt.setInt(place++, booking.getTtl());
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, booking.getPayload());
                    stmt.setNull(place++, Types.NULL);
                    stmt.setInt(place++, 0);

                    //FOR UPDATE
                    stmt.setObject(place++, booking.getAppointmentUid());
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, dueTimestamp);
                    stmt.setLong(place++, -1L);
                    stmt.setInt(place++, booking.getTtl());
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, booking.getPayload());
                    stmt.setNull(place++, Types.NULL);
                    stmt.setInt(place++, 0);

                    if (extraColumnsList != null) {
                        for (int i = 0; i < extraColumnsList.size(); i++) {
                            Object value = extraColumnsList.get(i).getValue();
                            if (value != null) {
                                stmt.setObject(place++, value);
                            } else {
                                stmt.setNull(place++, Types.NULL);
                            }
                        }
                    }
                    stmt.execute();
                }
            }
            conn.commit();
        }
    }

    static void bookPeriodic(
            String tableName,
            String periodicTableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            List<PeriodicAppointmentBooking> periodicBookings,
            boolean ignoreConflicts
    ) throws Exception {

        long nowEpochMs = clock.nowEpochMs();

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");

            ArrayList<Map.Entry<String, ?>> extraColumnsList;
            for (PeriodicAppointmentBooking periodic : periodicBookings) {

                if (periodic.getExtraParams() != null) {
                    extraColumnsList = new ArrayList<>(periodic.getExtraParams().entrySet());
                } else {
                    extraColumnsList = null;
                }
                String extraColumnsNames = buildExtraColumnsNames(extraColumnsList);
                String extraColumnsQuestionMarks = buildExtraColumnsQuestionMarks(extraColumnsList);
                String bookSql = String.format(
                        BOOK_INSERT_DONT_UPDATE_QUERY,
                        tableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );

                String periodicBookSql = String.format(
                        PERIODIC_BOOK_INSERT_QUERY,
                        periodicTableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );

                //Insert periodic booking
                try (PreparedStatement stmt = conn.prepareStatement(periodicBookSql)) {

                    int place = 1;

                    //FOR INSERT
                    stmt.setObject(place++, periodic.getAppointmentUid());
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setInt(place++, periodic.getPatternType().getId());
                    stmt.setString(place++, periodic.getPattern());
                    stmt.setInt(place++, periodic.getTtl());
                    stmt.setString(place++, periodic.getPayload());
                    stmt.setInt(place++, periodic.getKeyIteration());
                    stmt.setBoolean(place++, periodic.isSkipMissed());

                    if (extraColumnsList != null) {
                        for (int i = 0; i < extraColumnsList.size(); i++) {
                            Object value = extraColumnsList.get(i).getValue();
                            if (value != null) {
                                stmt.setObject(place++, value);
                            } else {
                                stmt.setNull(place++, Types.NULL);
                            }
                        }
                    }
                    stmt.execute();
                    if (stmt.getUpdateCount() == 0) {
                        if (ignoreConflicts) continue;
                        throw new ConflictingPeriodicBookingException(periodic.getAppointmentUid());
                    }
                }

                //Add first N iterations
                try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {
                    List<Long> dueTimestamps = getFirstDueTimestamps(periodic, nowEpochMs, periodic.getKeyIteration());
                    insertPeriodicIterations(periodic, dueTimestamps, stmt, extraColumnsList, nowEpochMs, 0);
                }

            }
            conn.commit();
        }
    }

    private static void insertPeriodicIterations(
            PeriodicAppointmentBooking periodic,
            List<Long> dueTimestamps,
            PreparedStatement stmt,
            ArrayList<Map.Entry<String, ?>> extraColumnsList,
            long nowEpochMs,
            int previousIteration) throws SQLException {
        long dueTimestamp;
        UUID iterationUid;
        int flags;
        if (periodic.isSkipMissed()) {
            flags = AjendaFlags.withFlags(AjendaFlags.SKIP_MISSED_FLAG);
        } else {
            flags = AjendaFlags.withFlags();
        }

        int i = 0;
        for (; i < dueTimestamps.size() - 1; i++) {

            dueTimestamp = dueTimestamps.get(i);

            iterationUid = UUIDType5.nameUUIDFromCustomString(
                    periodic.getAppointmentUid()
                            + "_" + dueTimestamp);

            setQueryParameters(
                    nowEpochMs,
                    periodic,
                    extraColumnsList,
                    stmt,
                    dueTimestamp,
                    iterationUid,
                    flags,
                    previousIteration + i + 1);
            stmt.addBatch();
        }

        flags = AjendaFlags.addFlags(flags, AjendaFlags.GEN_NEXT_FLAG);

        dueTimestamp = dueTimestamps.get(dueTimestamps.size() - 1);
        iterationUid = UUIDType5.nameUUIDFromCustomString(
                periodic.getAppointmentUid()
                        + "_" + dueTimestamp);

        setQueryParameters(nowEpochMs, periodic, extraColumnsList, stmt, dueTimestamp, iterationUid,
                flags, previousIteration + i + 1);
        stmt.addBatch();

        stmt.executeBatch();
    }

    static void bookNextIterations(
            AppointmentDue appointmentDue,
            String tableName,
            String periodicTableName,
            Connection conn,
            long nowEpochMs) throws Exception {

        if (appointmentDue.getPeriodicAppointmentUid() != null
                && AjendaFlags.isGenNext(appointmentDue.getFlags())) {

            PeriodicAppointmentBooking periodic = null;
            String getPeriodic = String.format(
                    PERIODIC_BOOK_SELECT_QUERY,
                    periodicTableName
            );

            try (PreparedStatement stmt = conn.prepareStatement(getPeriodic)) {

                stmt.setObject(1, appointmentDue.getPeriodicAppointmentUid());
                ResultSet resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    periodic = extractPeriodicAppointment(resultSet);
                }
            }

            if (periodic != null) {

                ArrayList<Map.Entry<String, ?>> extraColumnsList;
                if (periodic.getExtraParams() != null) {
                    extraColumnsList = new ArrayList<>(periodic.getExtraParams().entrySet());
                } else {
                    extraColumnsList = null;
                }
                String extraColumnsNames = buildExtraColumnsNames(extraColumnsList);
                String extraColumnsQuestionMarks = buildExtraColumnsQuestionMarks(extraColumnsList);
                String bookSql = String.format(
                        BOOK_INSERT_DONT_UPDATE_QUERY,
                        tableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );
                try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {

                    List<Long> dueTimestamps = getNextDueTimestamps(
                            periodic,
                            nowEpochMs,
                            appointmentDue.getDueTimestamp(),
                            periodic.getKeyIteration(),
                            new ArrayList<>(periodic.getKeyIteration())
                    );

                    insertPeriodicIterations(periodic, dueTimestamps, stmt, extraColumnsList, nowEpochMs, appointmentDue.getAttempts());
                }
            }
        }
    }

    private static void setQueryParameters(
            long nowEpochMs,
            PeriodicAppointmentBooking periodic,
            ArrayList<Map.Entry<String, ?>> extraColumnsList,
            PreparedStatement stmt,
            long dueTimestamp,
            UUID iterationUid,
            int flags,
            int attempts) throws SQLException {
        int place = 1;

        //FOR INSERT
        stmt.setObject(place++, iterationUid);
        stmt.setLong(place++, nowEpochMs);
        stmt.setLong(place++, dueTimestamp);
        stmt.setLong(place++, -1L);
        stmt.setInt(place++, periodic.getTtl());
        stmt.setInt(place++, attempts);
        stmt.setString(place++, periodic.getPayload());
        stmt.setObject(place++, periodic.getAppointmentUid());
        stmt.setInt(place++, flags);

        if (extraColumnsList != null) {
            for (int j = 0; j < extraColumnsList.size(); j++) {
                Object value = extraColumnsList.get(j).getValue();
                if (value != null) {
                    stmt.setObject(place++, value);
                } else {
                    stmt.setNull(place++, Types.NULL);
                }
            }
        }
    }

    private static List<Long> getFirstDueTimestamps(PeriodicAppointmentBooking booking, long nowEpochMs, int size) {
        List<Long> timestamps = new ArrayList<>();

        long firstExecutionTimestamp = booking.getStartTimestamp() > 0 ?
                booking.getStartTimestamp()
                //If negative, time is a relative delay
                : nowEpochMs - booking.getStartTimestamp();

        if (booking.isSkipMissed() && firstExecutionTimestamp < nowEpochMs) {
            firstExecutionTimestamp = nowEpochMs;
        }

        if (booking.getPatternType() != PeriodicPatternType.FIXED_RATE
                && booking.getPatternType() != PeriodicPatternType.FIXED_DELAY) {
            CronType cronType;
            switch (booking.getPatternType()) {
                case CRON_CRON4J:
                    cronType = CronType.CRON4J;
                    break;
                case CRON_QUARTZ:
                    cronType = CronType.QUARTZ;
                    break;
                case CRON_SPRING:
                    cronType = CronType.SPRING;
                    break;
                case CRON_UNIX:
                default:
                    cronType = CronType.UNIX;
                    break;
            }

            firstExecutionTimestamp = getCronNextDueTimestamp(booking, cronType, firstExecutionTimestamp);
        }

        timestamps.add(firstExecutionTimestamp);
        return getNextDueTimestamps(booking, nowEpochMs, firstExecutionTimestamp, size - 1, timestamps);

    }

    private static List<Long> getNextDueTimestamps(
            PeriodicAppointmentBooking bookings,
            long nowEpochMs,
            long previousDueDate,
            int size,
            List<Long> timestamps) {

        if (size <= 0) return timestamps;

        if (bookings.getPatternType() == PeriodicPatternType.FIXED_RATE) {
            long rate = Long.parseLong(bookings.getPattern(), 16);
            if (bookings.isSkipMissed()) {
                long due = previousDueDate +
                        Math.max(1L,
                                Math.round(0.5 + (nowEpochMs - previousDueDate) / (double) rate))
                                * rate;
                timestamps.add(due);
                long nextDue = due;
                while (//nextDue < nowEpochMs + size * lookAhead &&
                        timestamps.size() < size) {
                    nextDue += rate;
                    timestamps.add(nextDue);
                }
            } else {
                long due = previousDueDate + rate;
                timestamps.add(due);
                long nextDue = due + rate;
                while (//nextDue < nowEpochMs + size * lookAhead &&
                        timestamps.size() < size) {
                    timestamps.add(nextDue);
                    nextDue += rate;
                }
            }
        } else if (bookings.getPatternType() == PeriodicPatternType.FIXED_DELAY) {
            long rate = Long.parseLong(bookings.getPattern(), 16);
            timestamps.add(nowEpochMs + rate);
        } else {
            CronType cronType;
            switch (bookings.getPatternType()) {
                case CRON_CRON4J:
                    cronType = CronType.CRON4J;
                    break;
                case CRON_QUARTZ:
                    cronType = CronType.QUARTZ;
                    break;
                case CRON_SPRING:
                    cronType = CronType.SPRING;
                    break;
                case CRON_UNIX:
                default:
                    cronType = CronType.UNIX;
                    break;
            }

            if (bookings.isSkipMissed()) {
                long due = getCronNextDueTimestamp(bookings, cronType, nowEpochMs);
                timestamps.add(due);
                long nextDue = due;
                while (//nextDue < nowEpochMs + size * lookAhead &&
                        timestamps.size() < size) {
                    nextDue = getCronNextDueTimestamp(bookings, cronType, nextDue);
                    timestamps.add(nextDue);
                }
            } else {
                long due = getCronNextDueTimestamp(bookings, cronType, previousDueDate);
                timestamps.add(due);
                long nextDue = getCronNextDueTimestamp(bookings, cronType, due);
                while (//nextDue < nowEpochMs + size * lookAhead &&
                        timestamps.size() < size) {
                    timestamps.add(nextDue);
                    nextDue = getCronNextDueTimestamp(bookings, cronType, nextDue);
                }
            }
        }

        return timestamps;

    }

    private static long getCronNextDueTimestamp(PeriodicAppointmentBooking bookings, CronType cronType, long referenceTimestamp) {
        return ExecutionTime.forCron(
                new CronParser(
                        CronDefinitionBuilder.instanceDefinitionFor(cronType))
                        .parse(bookings.getPattern())).nextExecution(
                ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(referenceTimestamp), ZoneOffset.UTC))
                .orElse(
                        ZonedDateTime.ofInstant(
                                Instant.ofEpochMilli(Long.MAX_VALUE), ZoneOffset.UTC)).toInstant().toEpochMilli();
    }

    private static String buildExtraColumnsQuestionMarks(ArrayList<Map.Entry<String, ?>> extraColumns) {
        if (extraColumns == null || extraColumns.isEmpty()) return "";
        return ", " + extraColumns.stream().map(x -> "?").collect(Collectors.joining(", "));
    }

    private static String buildExtraColumnsNames(ArrayList<Map.Entry<String, ?>> extraColumns) {
        if (extraColumns == null || extraColumns.isEmpty()) return "";
        return ", " + extraColumns.stream().map(e -> "\"" + e.getKey() + "\"").collect(Collectors.joining(", "));
    }

    private static String buildQuestionMarks(int n) {
        return IntStream.range(0, n).mapToObj(x -> "?").collect(Collectors.joining(", "));
    }

}
