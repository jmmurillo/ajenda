package org.murillo.ajenda;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.murillo.ajenda.dto.*;
import org.murillo.ajenda.utils.UUIDType5;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
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
                    + "(uuid, creation_date, due_date, expiry_date, attempts, payload, periodic_uuid%s) "
                    + "VALUES (?, ?, ?, -1, ?, ?, ?%s) "
                    + "ON CONFLICT (uuid) DO UPDATE SET "
                    + "(uuid, creation_date, due_date, expiry_date, attempts, payload, periodic_uuid%s) "
                    + "= (?, ?, ?, -1, ?, ?, ?%s) ";

    private static final String PERIODIC_BOOK_INSERT_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, pattern_type, pattern, payload, skip_missed%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT DO NOTHING ";

    private static final String PERIODIC_BOOK_SELECT_QUERY =
            "SELECT * "
                    + "FROM %s "
                    + "WHERE uuid = ? ";

    private static final String CANCEL_BOOKINGS_QUERY =
            "DELETE "
                    + "FROM %s "
                    + "WHERE uuid IN (%s)"
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
                    //expiry stmt.setLong(place++, -1L);
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, booking.getPayload());
                    stmt.setNull(place++, Types.NULL);

                    //FOR UPDATE
                    stmt.setObject(place++, booking.getAppointmentUid());
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, dueTimestamp);
                    //expiry stmt.setLong(place++, -1L);
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, booking.getPayload());
                    stmt.setNull(place++, Types.NULL);

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
            int previousAttempts,
            List<PeriodicAppointmentBooking> periodicBookings
    ) throws Exception {

        long nowEpochMs = clock.nowEpochMs();

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");

            ArrayList<Map.Entry<String, ?>> extraColumnsList;
            for (PeriodicAppointmentBooking periodicBooking : periodicBookings) {

                if (periodicBooking.getExtraParams() != null) {
                    extraColumnsList = new ArrayList<>(periodicBooking.getExtraParams().entrySet());
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

                String periodicBookSql = String.format(
                        PERIODIC_BOOK_INSERT_QUERY,
                        periodicTableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );

                long firstDueTimestamp = getFirstDueTimestamp(periodicBooking, nowEpochMs);
                UUID iterationUid = UUIDType5.nameUUIDFromCustomString(
                        periodicBooking.getAppointmentUid()
                                + "_" + firstDueTimestamp);

                //Insert periodic booking
                try (PreparedStatement stmt = conn.prepareStatement(periodicBookSql)) {

                    int place = 1;

                    //FOR INSERT
                    stmt.setObject(place++, periodicBooking.getAppointmentUid());
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setInt(place++, periodicBooking.getPatternType().getId());
                    stmt.setString(place++, periodicBooking.getPattern());
                    stmt.setString(place++, periodicBooking.getPayload());
                    stmt.setBoolean(place++, periodicBooking.isSkipMissed());

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
                        throw new ConflictingPeriodicBookingException(periodicBooking.getAppointmentUid());
                    }
                }

                //Add first iteration
                try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {

                    int place = 1;
                    //FOR INSERT
                    stmt.setObject(place++, iterationUid);
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, firstDueTimestamp);
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, periodicBooking.getPayload());
                    stmt.setObject(place++, periodicBooking.getAppointmentUid());

                    //FOR UPDATE
                    stmt.setObject(place++, iterationUid);
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, firstDueTimestamp);
                    stmt.setInt(place++, previousAttempts);
                    stmt.setString(place++, periodicBooking.getPayload());
                    stmt.setObject(place++, periodicBooking.getAppointmentUid());

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

    static void bookNextIteration(
            AppointmentDue appointmentDue,
            String tableName,
            String periodicTableName,
            Connection conn,
            long nowEpochMs) throws Exception {

        if (appointmentDue.getPeriodicAppointmentUid() != null) {
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
                        BOOK_INSERT_QUERY,
                        tableName,
                        extraColumnsNames,
                        extraColumnsQuestionMarks,
                        extraColumnsNames,
                        extraColumnsQuestionMarks
                );
                try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {
                    int place = 1;

                    long dueTimestamp = getNextDueTimestamp(
                            periodic,
                            nowEpochMs,
                            appointmentDue.getDueTimestamp()
                    );

                    UUID iterationUid = UUIDType5.nameUUIDFromCustomString(
                            periodic.getAppointmentUid()
                                    + "_" + dueTimestamp);

                    //FOR INSERT
                    stmt.setObject(place++, iterationUid);
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, dueTimestamp);
                    stmt.setInt(place++, 0);
                    stmt.setString(place++, periodic.getPayload());
                    stmt.setObject(place++, periodic.getAppointmentUid());

                    //FOR UPDATE
                    stmt.setObject(place++, iterationUid);
                    stmt.setLong(place++, nowEpochMs);
                    stmt.setLong(place++, dueTimestamp);
                    stmt.setInt(place++, 0);
                    stmt.setString(place++, periodic.getPayload());
                    stmt.setObject(place++, periodic.getAppointmentUid());

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
        }
    }

    private static long getFirstDueTimestamp(PeriodicAppointmentBooking bookings, long nowEpochMs) {
        long firstExecutionTimestamp = bookings.getStartTimestamp() > 0 ?
                bookings.getStartTimestamp()
                //If negative, time is a relative delay
                : nowEpochMs - bookings.getStartTimestamp();

        if (bookings.isSkipMissed() && firstExecutionTimestamp < nowEpochMs) {
            firstExecutionTimestamp = nowEpochMs;
        }

        if (bookings.getPatternType() == PeriodicPatternType.FIXED_RATE || bookings.getPatternType() == PeriodicPatternType.FIXED_DELAY) {
            return firstExecutionTimestamp;
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

            return ExecutionTime.forCron(
                    new CronParser(
                            CronDefinitionBuilder.instanceDefinitionFor(cronType))
                            .parse(bookings.getPattern())).nextExecution(
                    ZonedDateTime.ofInstant(
                            Instant.ofEpochMilli(firstExecutionTimestamp), ZoneOffset.UTC))
                    .orElse(
                            ZonedDateTime.ofInstant(
                                    Instant.ofEpochMilli(Long.MAX_VALUE), ZoneOffset.UTC)).toInstant().toEpochMilli();
        }

    }

    private static long getNextDueTimestamp(PeriodicAppointmentBooking bookings, long nowEpochMs, long previousDueDate) {
        if (bookings.getPatternType() == PeriodicPatternType.FIXED_RATE) {
            long rate = Long.parseLong(bookings.getPattern());
            if (bookings.isSkipMissed()) {
                return previousDueDate +
                        Math.max(1L,
                                Math.round(0.5 + (nowEpochMs - previousDueDate) / (double) rate))
                                * rate;
            } else {
                return previousDueDate + rate;
            }
        } else if (bookings.getPatternType() == PeriodicPatternType.FIXED_DELAY) {
            long rate = Long.parseLong(bookings.getPattern());
            return nowEpochMs + rate;
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

            long referenceTimestamp = bookings.isSkipMissed() ?
                    nowEpochMs
                    : previousDueDate;

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
