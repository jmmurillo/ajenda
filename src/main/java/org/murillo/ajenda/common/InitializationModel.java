package org.murillo.ajenda.common;

import java.sql.Connection;
import java.sql.Statement;

import static org.murillo.ajenda.utils.Common.getTableNameForTopic;

public class InitializationModel {

    public static final String TABLE_FOR_TOPIC_QUERY =
            "CREATE TABLE IF NOT EXISTS %s ( "
                    + "uuid             UUID PRIMARY KEY, "
                    + "creation_date    BIGINT, "
                    + "due_date         BIGINT, "
                    + "expiry_date      BIGINT, "
                    + "attempts         INTEGER, "
                    + "payload          TEXT "
                    + ") WITH (fillfactor = 70) ";

    public static final String CREATE_DUE_DATE_INDEX_QUERY =
            "CREATE INDEX IF NOT EXISTS idx_%s_duedate ON %s ("
                    + "due_date ASC "
                    + ") WITH (fillfactor = 70) ";

    public static void initTableForTopic(ConnectionFactory<?> dataSource, String topic) throws Exception {
        String tableName = getTableNameForTopic(topic);
        try (Connection conn = dataSource.getConnection()) {
            if (conn.getAutoCommit()) throw new IllegalStateException("Connection must have auto-commit disabled");
            String createTableSql = String.format(
                    TABLE_FOR_TOPIC_QUERY,
                    tableName);
            String createIndexSql = String.format(
                    CREATE_DUE_DATE_INDEX_QUERY,
                    tableName, tableName);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSql);
                stmt.execute(createIndexSql);
                conn.commit();
            }
        }
    }    
    
}
