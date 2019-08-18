package org.murillo.ajenda.booking;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.common.ConnectionFactory;
import org.murillo.ajenda.dto.*;
import org.murillo.ajenda.handling.Utils;
import org.murillo.ajenda.utils.UUIDType5;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BookModel<T extends Connection> {

    private static final Pattern splitPattern = Pattern.compile(":", Pattern.LITERAL);

    public static final String BOOK_INSERT_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, due_date, expiry_date, attempts, payload, periodic_uuid%s) "
                    + "VALUES (?, ?, ?, -1, ?, ?, ?%s) "
                    + "ON CONFLICT (uuid) DO UPDATE SET "
                    + "(uuid, creation_date, due_date, expiry_date, attempts, payload, periodic_uuid%s) "
                    + "= (?, ?, ?, -1, ?, ?, ?%s) ";

    public static final String PERIODIC_BOOK_INSERT_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, pattern_type, pattern, payload, skip_missed%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT DO NOTHING ";

    public static final String PERIODIC_BOOK_SELECT_QUERY =
            "SELECT * "
                    + "FROM %s "
                    + "WHERE uuid = ? ";

    public static void book(
            String tableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            AppointmentBooking booking,
            int previousAttempts) throws Exception {
        rebook(tableName, connectionFactory, clock, booking, previousAttempts, null);
    }
    
    public static void rebook(
            String tableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            AppointmentBooking booking,
            int previousAttempts,
            UUID periodicUid) throws Exception {

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

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {
                int place = 1;

                //FOR INSERT
                stmt.setObject(place++, booking.getAppointmentUid());
                long nowEpochMs = clock.nowEpochMs();
                stmt.setLong(place++, nowEpochMs);
                long dueTimestamp = booking.getDueTimestamp() > 0 ?
                        booking.getDueTimestamp()
                        //If negative, time is a relative delay
                        : nowEpochMs - booking.getDueTimestamp();
                stmt.setLong(place++, dueTimestamp);
                //expiry stmt.setLong(place++, -1L);
                stmt.setInt(place++, previousAttempts);
                stmt.setString(place++, booking.getPayload());
                if(periodicUid != null){
                    stmt.setObject(place++, periodicUid);
                }else{
                    stmt.setNull(place++, Types.NULL);
                }

                //FOR UPDATE
                stmt.setObject(place++, booking.getAppointmentUid());
                stmt.setLong(place++, nowEpochMs);
                stmt.setLong(place++, dueTimestamp);
                //expiry stmt.setLong(place++, -1L);
                stmt.setInt(place++, previousAttempts);
                stmt.setString(place++, booking.getPayload());
                if(periodicUid != null){
                    stmt.setObject(place++, periodicUid);
                }else{
                    stmt.setNull(place++, Types.NULL);
                }

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
                conn.commit();
            }
        }
    }

    public static void bookPeriodic(
            String tableName,
            String periodicTableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            PeriodicAppointmentBooking periodicBooking,
            int previousAttempts) throws Exception {

        long nowEpochMs = clock.nowEpochMs();
        long firstDueTimestamp = getFirstDueTimestamp(periodicBooking, nowEpochMs);
        ArrayList<Map.Entry<String, ?>> extraColumnsList;
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

        UUID iterationUid = UUIDType5.nameUUIDFromCustomString(
                periodicBooking.getAppointmentUid()
                        + "_" + firstDueTimestamp);

        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");

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

                //FOR UPDATE
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
                stmt.setObject(place++, iterationUid);
                stmt.setLong(place++, nowEpochMs);
                stmt.setLong(place++, firstDueTimestamp);
                //expiry stmt.setLong(place++, -1L);
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
            conn.commit();
        }
    }

    private void queueNextIteration(
            AppointmentDue appointmentDue,
            String tableName,
            String periodicTableName,
            Connection conn,
            Clock clock) throws Exception {

        if (appointmentDue.getAppointmentUid() != null) {
            PeriodicAppointmentBooking periodic = null;
            String getPeriodic = String.format(
                    PERIODIC_BOOK_SELECT_QUERY,
                    periodicTableName
            );


            try (PreparedStatement stmt = conn.prepareStatement(getPeriodic)) {
                ResultSet resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    periodic = Utils.extractPeriodicAppointment(resultSet);
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
                long nowEpochMs = clock.nowEpochMs();
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

    private static long getFirstDueTimestamp(PeriodicAppointmentBooking booking, long nowEpochMs) {
        long firstExecutionTimestamp = booking.getStartTimestamp() > 0 ?
                booking.getStartTimestamp()
                //If negative, time is a relative delay
                : nowEpochMs - booking.getStartTimestamp();

        if (booking.isSkipMissed() && firstExecutionTimestamp < nowEpochMs) {
            firstExecutionTimestamp = nowEpochMs;
        }

        if (booking.getPatternType() == PeriodicPatternType.FIXED_RATE || booking.getPatternType() == PeriodicPatternType.FIXED_DELAY) {
            return firstExecutionTimestamp;
        } else {
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

            return ExecutionTime.forCron(
                    new CronParser(
                            CronDefinitionBuilder.instanceDefinitionFor(cronType))
                            .parse(booking.getPattern())).nextExecution(
                    ZonedDateTime.ofInstant(
                            Instant.ofEpochMilli(firstExecutionTimestamp), ZoneOffset.UTC))
                    .orElse(
                            ZonedDateTime.ofInstant(
                                    Instant.ofEpochMilli(Long.MAX_VALUE), ZoneOffset.UTC)).toInstant().toEpochMilli();
        }

    }

    private static long getNextDueTimestamp(PeriodicAppointmentBooking booking, long nowEpochMs, long previousDueDate) {
        if (booking.getPatternType() == PeriodicPatternType.FIXED_RATE) {
            long rate = Long.parseLong(booking.getPattern());
            if (booking.isSkipMissed()) {
                return previousDueDate + Math.round(0.5 + (nowEpochMs - previousDueDate) / (double) rate) * rate;
            } else {
                return previousDueDate + rate;
            }
        } else if (booking.getPatternType() == PeriodicPatternType.FIXED_DELAY) {
            long rate = Long.parseLong(booking.getPattern());
            return nowEpochMs + rate;
        } else {
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

            long referenceTimestamp = booking.isSkipMissed() ?
                    nowEpochMs
                    : previousDueDate;

            return ExecutionTime.forCron(
                    new CronParser(
                            CronDefinitionBuilder.instanceDefinitionFor(cronType))
                            .parse(booking.getPattern())).nextExecution(
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
}
