package org.murillo.ajenda.test.utils;

import org.murillo.ajenda.dto.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    public static boolean doesTableExist(ConnectionFactory ds, String tableName) throws Exception {
        String query =
                "SELECT EXISTS (" +
                        "SELECT 1 " +
                        "FROM   information_schema.tables " +
                        "WHERE  table_schema = 'public' " +
                        "AND    table_name = ?); ";

        try (Connection connection = ds.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, tableName);
                ResultSet resultSet = statement.executeQuery();
                return resultSet.next() && resultSet.getBoolean(1);
            }
        }
    }

    public static List<String> getColumnNamesForTable(ConnectionFactory ds, String tableName) throws Exception {

        String query =
                "SELECT column_name " +
                        "FROM   information_schema.columns " +
                        "WHERE  table_schema = 'public' " +
                        "AND    table_name = ?; ";

        ArrayList<String> columns = new ArrayList<>();

        try (Connection connection = ds.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setString(1, tableName);
                ResultSet resultSet = statement.executeQuery();
                while (resultSet.next()) {
                    columns.add(resultSet.getString(1));
                }
            }
        }
        return columns;
    }

}
