package org.murillo.ajenda.core;

public class ConnectionFactoryFactory {

    public static ConnectionFactory from(HibernateSessionFactory hibernateSessionFactory){
        return () -> new HibernateConnectionWrapper(hibernateSessionFactory);
    }

    public static ConnectionFactory from(JdbcConnectionFactory jdbcConnectionFactory){
        return () -> new JdbcConnectionWrapper(jdbcConnectionFactory);
    }

}
