package org.murillo.ajenda;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.murillo.ajenda.common.ConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class HikariCPDataSource implements ConnectionFactory {

    private HikariConfig config = new HikariConfig();
    private HikariDataSource ds;

    public HikariCPDataSource() {
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
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