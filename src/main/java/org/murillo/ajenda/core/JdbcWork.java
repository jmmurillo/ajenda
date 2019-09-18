package org.murillo.ajenda.core;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcWork<R> {
    R execute(Connection connection) throws SQLException;
}
