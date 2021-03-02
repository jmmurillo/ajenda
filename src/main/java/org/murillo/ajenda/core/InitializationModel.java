package org.murillo.ajenda.core;

import java.sql.*;
import java.util.HashMap;

public class InitializationModel {

    public static final String TABLE_FOR_TOPIC_QUERY =
            "CREATE TABLE IF NOT EXISTS %s ( "
                    + "uuid             UUID PRIMARY KEY, "
                    + "creation_date    BIGINT, "
                    + "due_date         BIGINT, "
                    + "timeout_date     BIGINT, "
                    + "ttl              BIGINT, "
                    + "attempts         INTEGER, "
                    + "payload          TEXT, "
                    + "periodic_uuid    UUID, "
                    + "flags            INTEGER, "
                    + "version          SMALLINT DEFAULT 0 "
                    + ") WITH (fillfactor = 70) ";

    public static final String TABLE_FOR_PERIODIC_TOPIC_QUERY =
            "CREATE TABLE IF NOT EXISTS %s ( "
                    + "uuid             UUID PRIMARY KEY, "
                    + "creation_date    BIGINT, "
                    + "pattern_type     INTEGER, "
                    + "pattern          TEXT, "
                    + "ttl              BIGINT, "
                    + "payload          TEXT, "
                    + "key_iteration    INTEGER, "
                    + "skip_missed      BOOLEAN "
                    + ")";

    public static final String CREATE_DUE_DATE_INDEX_QUERY =
            "CREATE INDEX IF NOT EXISTS idx_%s_duedate ON %s ("
                    + "due_date ASC "
                    + ") WITH (fillfactor = 70) ";

    public static final String COLUMNS_QUERY =
            "SELECT column_name, data_type " +
                    "FROM   information_schema.columns " +
                    "WHERE  table_schema = ? " +
                    "AND    table_name = ? ";

    public static final String ADD_COLUMN_IF_NOT_EXISTS =
            "ALTER TABLE %s "
                    + "ADD COLUMN IF NOT EXISTS %s %s ";

    public static void initTableForTopic(ConnectionFactory dataSource, String topic, String schemaName, String tableName, String periodicTableName) throws Exception {

        try (ConnectionWrapper connw = dataSource.getConnection()) {
            connw.doWork(connection -> {
                if (connection.getAutoCommit())
                    throw new IllegalStateException("Connection must have auto-commit disabled");
                String createTableSql = String.format(
                        TABLE_FOR_TOPIC_QUERY,
                        tableName);
                String createPeriodicTableSql = String.format(
                        TABLE_FOR_PERIODIC_TOPIC_QUERY,
                        periodicTableName);
                String createIndexSql = String.format(
                        CREATE_DUE_DATE_INDEX_QUERY,
                        tableName, tableName);
                String addVersionColumnSql = String.format(ADD_COLUMN_IF_NOT_EXISTS,
                        tableName, "version", "SMALLINT DEFAULT 0");
                try (Statement stmt = connection.createStatement()) {

                    stmt.execute(createTableSql);

                    //Previous ajenda versions did not include this column, so add it to previously existing main table
                    stmt.execute(addVersionColumnSql);

                    //Assert table exists and has expected columns
                    HashMap<String, String> columnsForTable = getColumnsForTable(connection, tableName, schemaName);
                    ensureColumnAndType(columnsForTable, tableName, "uuid", "UUID");
                    ensureColumnAndType(columnsForTable, tableName, "creation_date", "BIGINT");
                    ensureColumnAndType(columnsForTable, tableName, "due_date", "BIGINT");
                    ensureColumnAndType(columnsForTable, tableName, "timeout_date", "BIGINT");
                    ensureColumnAndType(columnsForTable, tableName, "ttl", "BIGINT");
                    ensureColumnAndType(columnsForTable, tableName, "attempts", "INTEGER");
                    ensureColumnAndType(columnsForTable, tableName, "payload", "TEXT");
                    ensureColumnAndType(columnsForTable, tableName, "periodic_uuid", "UUID");
                    ensureColumnAndType(columnsForTable, tableName, "flags", "INTEGER");
                    ensureColumnAndType(columnsForTable, tableName, "version", "SMALLINT");

                    stmt.execute(createPeriodicTableSql);
                    //Assert periodic table exists and has expected columns
                    columnsForTable = getColumnsForTable(connection, periodicTableName, schemaName);
                    ensureColumnAndType(columnsForTable, periodicTableName, "uuid", "UUID");
                    ensureColumnAndType(columnsForTable, periodicTableName, "creation_date", "BIGINT");
                    ensureColumnAndType(columnsForTable, periodicTableName, "pattern_type", "INTEGER");
                    ensureColumnAndType(columnsForTable, periodicTableName, "pattern", "TEXT");
                    ensureColumnAndType(columnsForTable, periodicTableName, "ttl", "BIGINT");
                    ensureColumnAndType(columnsForTable, periodicTableName, "payload", "TEXT");
                    ensureColumnAndType(columnsForTable, periodicTableName, "key_iteration", "INTEGER");
                    ensureColumnAndType(columnsForTable, periodicTableName, "skip_missed", "BOOLEAN");
                    stmt.execute(createIndexSql);
                }
                return null;
            });
            connw.commit();
        }
    }

    private static void ensureColumnAndType(HashMap<String, String> columnsForTable, String tableName, String columnName, String expectedType) {
        String actualType = columnsForTable.get(columnName);
        if (actualType == null) {
            throw new IllegalStateException(String.format("Column %s does not exist in Ajenda table %s", columnName, tableName));
        } else if (!actualType.equalsIgnoreCase(expectedType)) {
            throw new IllegalStateException(String.format("Column %s in Ajenda table %s does not match the expected type: " +
                    "expected %s but found %s", columnName, tableName, expectedType, actualType));
        }
    }

    public static boolean doesTableExist(ConnectionFactory ds, String tableName) throws Exception {
        String query =
                "SELECT EXISTS (" +
                        "SELECT 1 " +
                        "FROM   information_schema.tables " +
                        "WHERE  table_schema = 'public' " +
                        "AND    table_name = ?) ";

        try (ConnectionWrapper connw = ds.getConnection()) {
            return connw.doWork(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.setString(1, tableName);
                    ResultSet resultSet = statement.executeQuery();
                    return resultSet.next() && resultSet.getBoolean(1);
                }
            });
        }
    }

    public static HashMap<String, String> getColumnsForTable(Connection conn, String tableName, String schemaName) throws SQLException {

        HashMap<String, String> columns = new HashMap<>();

        try (PreparedStatement statement = conn.prepareStatement(COLUMNS_QUERY)) {
            statement.setString(1, schemaName);
            statement.setString(2, tableName);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                columns.put(
                        resultSet.getString(1),
                        resultSet.getString(2));
            }

        }
        return columns;
    }

}
