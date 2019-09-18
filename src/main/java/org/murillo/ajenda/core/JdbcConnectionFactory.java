package org.murillo.ajenda.core;

import java.sql.Connection;

@FunctionalInterface
public interface JdbcConnectionFactory {

    Connection getConnection() throws Exception;

}
