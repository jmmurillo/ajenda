package org.murillo.ajenda.core;

import java.sql.SQLException;

public interface ConnectionWrapper extends AutoCloseable {

    <R> R doWork(JdbcWork<R> jdbcWork) throws Exception;

    void commit() throws SQLException;
}

