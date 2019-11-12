package org.murillo.ajenda.core;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.resource.transaction.spi.TransactionStatus;

import java.sql.SQLException;

public class HibernateConnectionWrapper implements ConnectionWrapper {

    private HibernateSessionFactory sessionFactory;
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
                this.session = sessionFactory.getSession();
                this.transaction = session.getTransaction();
                if (transaction.getStatus() == null || transaction.getStatus().isOneOf(TransactionStatus.NOT_ACTIVE)) {
                    this.transaction = session.beginTransaction();
                }
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
