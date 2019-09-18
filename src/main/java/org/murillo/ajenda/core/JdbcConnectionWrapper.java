package org.murillo.ajenda.core;

import java.sql.Connection;
import java.sql.SQLException;

public class JdbcConnectionWrapper implements ConnectionWrapper {

    JdbcConnectionFactory connectionFactory;
    private Connection connection;
    private boolean committed = false;

    public JdbcConnectionWrapper(JdbcConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Override
    public synchronized <R> R doWork(JdbcWork<R> jdbcWork) throws Exception {
        synchronized (this) {
            if (connection == null) {
                connection = connectionFactory.getConnection();
                if (connection.getAutoCommit()) {
                    connection = null;
                    throw new IllegalStateException("Connection must have auto-commit disabled");
                }
            }
        }
        return jdbcWork.execute(connection);
    }

    @Override
    public void commit() throws SQLException {
        synchronized (this) {
            this.connection.commit();
            this.committed = true;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (!committed) this.connection.rollback();
            this.connection.close();
            this.connectionFactory = null;
            this.connection = null;
        }
    }
}
