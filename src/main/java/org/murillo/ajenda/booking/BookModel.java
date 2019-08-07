package org.murillo.ajenda.booking;

import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.common.ConnectionFactory;
import org.murillo.ajenda.dto.AppointmentBooking;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class BookModel<T extends Connection> {

    public static final String BOOK_QUERY =
            "INSERT INTO %s "
                    + "(uuid, creation_date, due_date, expiry_date, attempts, payload%s) "
                    + "VALUES (?, ?, ?, ?, ?, ?%s) "
                    + "ON CONFLICT DO NOTHING ";

    public static void book(
            String tableName,
            ConnectionFactory<?> connectionFactory,
            Clock clock,
            AppointmentBooking booking,
            int previousAttempts) throws Exception {
        try (Connection conn = connectionFactory.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            ArrayList<Map.Entry<String, ?>> extraColumnsList;
            if (booking.getExtraParams() != null) {
                extraColumnsList = new ArrayList<>(booking.getExtraParams().entrySet());
            } else {
                extraColumnsList = null;
            }
            String bookSql = String.format(
                    BOOK_QUERY,
                    tableName,
                    buildExtraColumnsNames(extraColumnsList),
                    buildExtraColumnsQuestionMarks(extraColumnsList)
            );

            try (PreparedStatement stmt = conn.prepareStatement(bookSql)) {
                int place = 1;
                stmt.setObject(place++, booking.getAppointmentUid());
                stmt.setLong(place++, clock.nowEpochMs());
                stmt.setLong(place++, booking.getDueTimestamp() > 0 ?
                        booking.getDueTimestamp()
                        //If negative, time is a relative delay
                        : clock.nowEpochMs() - booking.getDueTimestamp());
                stmt.setLong(place++, 0L);
                stmt.setInt(place++, previousAttempts);
                stmt.setString(place++, booking.getPayload());
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

    private static String buildExtraColumnsQuestionMarks(ArrayList<Map.Entry<String, ?>> extraColumns) {
        if (extraColumns == null || extraColumns.isEmpty()) return "";
        return ", " + extraColumns.stream().map(x -> "?").collect(Collectors.joining(", "));
    }

    private static String buildExtraColumnsNames(ArrayList<Map.Entry<String, ?>> extraColumns) {
        if (extraColumns == null || extraColumns.isEmpty()) return "";
        return ", " + extraColumns.stream().map(e -> "\"" + e.getKey() + "\"").collect(Collectors.joining(", "));
    }
}
