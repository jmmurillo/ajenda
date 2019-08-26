package org.murillo.ajenda;

import org.murillo.ajenda.dto.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

public class InitializationModel {

    public static final String TABLE_FOR_TOPIC_QUERY =
            "CREATE TABLE IF NOT EXISTS %s ( "
                    + "uuid             UUID PRIMARY KEY, "
                    + "creation_date    BIGINT, "
                    + "due_date         BIGINT, "
                    + "expiry_date      BIGINT, "
                    + "attempts         INTEGER, "
                    + "payload          TEXT, "
                    + "periodic_uuid    UUID "
                    + ") WITH (fillfactor = 70) ";

    public static final String TABLE_FOR_PERIODIC_TOPIC_QUERY =
            "CREATE TABLE IF NOT EXISTS %s ( "
                    + "uuid             UUID PRIMARY KEY, "
                    + "creation_date    BIGINT, "
                    + "pattern_type     INTEGER, "
                    + "pattern          TEXT, "
                    + "payload          TEXT, "
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

    public static void initTableForTopic(ConnectionFactory<?> dataSource, String topic, String schemaName, String tableName, String periodicTableName) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            String createTableSql = String.format(
                    TABLE_FOR_TOPIC_QUERY,
                    tableName);
            String createPeriodicTableSql = String.format(
                    TABLE_FOR_PERIODIC_TOPIC_QUERY,
                    periodicTableName);            
            String createIndexSql = String.format(
                    CREATE_DUE_DATE_INDEX_QUERY,
                    tableName, tableName);
            try (Statement stmt = conn.createStatement()) {
                
                stmt.execute(createTableSql);
                //Assert table exists and has expected columns
                HashMap<String, String> columnsForTable = getColumnsForTable(conn, tableName, schemaName);
                ensureColumnAndType(columnsForTable, tableName, "uuid", "UUID");
                ensureColumnAndType(columnsForTable, tableName, "creation_date", "BIGINT");
                ensureColumnAndType(columnsForTable, tableName, "due_date", "BIGINT");
                ensureColumnAndType(columnsForTable, tableName, "expiry_date", "BIGINT");
                ensureColumnAndType(columnsForTable, tableName, "attempts", "INTEGER");
                ensureColumnAndType(columnsForTable, tableName, "payload", "TEXT");
                ensureColumnAndType(columnsForTable, tableName, "periodic_uuid", "UUID");

                stmt.execute(createPeriodicTableSql);
                //Assert periodic table exists and has expected columns
                columnsForTable = getColumnsForTable(conn, periodicTableName, schemaName);
                ensureColumnAndType(columnsForTable, periodicTableName, "uuid", "UUID");
                ensureColumnAndType(columnsForTable, periodicTableName, "creation_date", "BIGINT");
                ensureColumnAndType(columnsForTable, periodicTableName, "pattern_type", "INTEGER");
                ensureColumnAndType(columnsForTable, periodicTableName, "pattern", "TEXT");                
                ensureColumnAndType(columnsForTable, periodicTableName, "payload", "TEXT");                
                
                stmt.execute(createIndexSql);
                conn.commit();
            }
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

        try (Connection connection = ds.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, tableName);
                ResultSet resultSet = statement.executeQuery();
                return resultSet.next() && resultSet.getBoolean(1);
            }
        }
    }

    public static HashMap<String, String> getColumnsForTable(Connection conn, String tableName, String schemaName) throws Exception {

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
