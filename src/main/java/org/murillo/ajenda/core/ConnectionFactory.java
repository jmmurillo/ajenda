package org.murillo.ajenda.core;

@FunctionalInterface
public interface ConnectionFactory {

    ConnectionWrapper getConnection() throws Exception;

}
