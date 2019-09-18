package org.murillo.ajenda.core;

import org.hibernate.Session;

@FunctionalInterface
public interface HibernateSessionFactory {

    Session getSession() throws Exception;
    
}
