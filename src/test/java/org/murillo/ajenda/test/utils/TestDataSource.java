package org.murillo.ajenda.test.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.murillo.ajenda.dto.ConnectionFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class TestDataSource implements ConnectionFactory {
    
    private HikariConfig config;
    private HikariDataSource ds;

    public TestDataSource(DataSource postgresDatabase) {

        config = new HikariConfig();
        config.setDataSource(postgresDatabase);
        //config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
//        config.setUsername("postgres");
//        config.setPassword("mysecretpassword");
//        config.addDataSourceProperty("cachePrepStmts", "true");
//        config.addDataSourceProperty("prepStmtCacheSize", "250");
//        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setAutoCommit(false);
        ds = new HikariDataSource(config);
    }

    public TestDataSource(String jdbcUrl) {

        config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername("postgres");
        config.setPassword("mysecretpassword");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setAutoCommit(false);
        ds = new HikariDataSource(config);
    }

    public Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

}