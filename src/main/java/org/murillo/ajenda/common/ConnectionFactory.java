package org.murillo.ajenda.common;

import java.sql.Connection;

@FunctionalInterface
public interface ConnectionFactory<T extends Connection> {

    T getConnection() throws Exception;
    
}
