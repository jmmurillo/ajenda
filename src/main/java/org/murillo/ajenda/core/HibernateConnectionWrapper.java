package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.hibernate.Transaction;

import java.sql.SQLException;

public class HibernateConnectionWrapper implements ConnectionWrapper {

    HibernateSessionFactory sessionFactory;
    private Session session;
    private Transaction transaction;
    private boolean committed = false;

    public HibernateConnectionWrapper(HibernateSessionFactory hibernateSessionFactory) {
        this.sessionFactory = hibernateSessionFactory;
    }

    @Override
    public synchronized <R> R doWork(JdbcWork<R> jdbcWork) throws Exception {
        synchronized (this) {
            if (transaction == null) {
                session = sessionFactory.getSession();
                transaction = session.beginTransaction();
            }
        }
        return session.doReturningWork(jdbcWork::execute);
    }

    @Override
    public void commit() throws SQLException {
        synchronized (this) {
            this.transaction.commit();
            this.committed = true;
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (this) {
            try {
                if (!committed && transaction != null) {
                    this.transaction.rollback();
                }
            } finally {
                this.session.close();
                this.sessionFactory = null;
                this.session = null;
                this.transaction = null;
            }

        }
    }
}
