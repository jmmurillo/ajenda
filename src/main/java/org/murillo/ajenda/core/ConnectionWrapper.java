package org.murillo.ajenda.core;

import java.sql.SQLException;

public interface ConnectionWrapper extends AutoCloseable {

    <R> R doWork(JdbcWork<R> jdbcWork) throws Exception;

    void commit() throws SQLException;

    default ConnectionWrapper recursiveConnectionWrapper(){
        return new ConnectionWrapper() {
            @Override
            public <R> R doWork(JdbcWork<R> jdbcWork) throws SQLException {
                try {
                    return ConnectionWrapper.this.doWork(jdbcWork);
                } catch (SQLException e) {
                    throw e;
                } catch (Exception e) {
                    throw new SQLException("Exception wrapper", e);
                }
            }

            @Override
            public void commit() throws SQLException {
                //do nothing
            }

            @Override
            public void close() throws Exception {
                //do nothing
            }
        };
    }
}

