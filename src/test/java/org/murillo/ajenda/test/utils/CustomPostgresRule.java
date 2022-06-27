package org.murillo.ajenda.test.utils;

import com.cronutils.utils.Preconditions;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class CustomPostgresRule extends ExternalResource {

    private volatile EmbeddedPostgres epg;
    private volatile Connection postgresConnection;
    private volatile int port = 0;

    public CustomPostgresRule(int port) {
        super();
        this.port = port;
    }

    public CustomPostgresRule() {
    }


    @Override
    protected void before() throws Throwable {
        super.before();
        this.epg = EmbeddedPostgres.builder().setPort(this.port).start();
        this.port = this.epg.getPort();
        this.postgresConnection = this.epg.getPostgresDatabase().getConnection();
    }

    public EmbeddedPostgres getEmbeddedPostgres() {
        EmbeddedPostgres epg = this.epg;
        Preconditions.checkState(epg != null, "JUnit test not started yet!");
        return epg;
    }

    public int getPort() {
        return port;
    }

    @Override
    protected void after() {
        try {
            this.postgresConnection.close();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
        try {
            this.epg.close();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
