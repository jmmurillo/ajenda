package org.murillo.ajenda.test;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.dto.AppointmentBookingBuilder;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.dto.UnhandledAppointmentException;
import org.murillo.ajenda.handling.AjendaScheduler;
import org.murillo.ajenda.test.utils.TestDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestAtMostOnce {

    @ClassRule
    public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

    @Before
    public void clearDB() throws SQLException {
        try (Connection connection = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("DO $$ DECLARE "
                        + "r RECORD ;"
                        + "BEGIN "
                        + "   FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP "
                        + "      EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; "
                        + "   END LOOP; "
                        + "END $$; ");
                connection.commit();
            }
        }
    }

    @org.junit.Test
    public void test_simple_book_and_handle() throws Exception {
        String topic = "prueba";
        String payload = UUID.randomUUID().toString();
        TestDataSource dataSource = new TestDataSource(
                pg.getEmbeddedPostgres().getPostgresDatabase());

        simpleBookAppointment(topic, payload, dataSource);

        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                new Clock() {
                });

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });

        Assert.assertEquals(1, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());

        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(1, read.size());
    }

    @org.junit.Test
    public void test_simple_book_and_handle_rebook() throws Exception {
        String topic = "prueba";
        String payload = UUID.randomUUID().toString();
        TestDataSource dataSource = new TestDataSource(
                pg.getEmbeddedPostgres().getPostgresDatabase());

        simpleBookAppointment(topic, payload, dataSource);

        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                new Clock() {
                });

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().once(10).readAtMostOnce(true, appointmentDue -> {
            read.add(appointmentDue);
            throw new UnhandledAppointmentException();
        });

        Assert.assertEquals(1, read.size());

        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
            throw new UnhandledAppointmentException();
        });

        Assert.assertEquals(2, read.size());

        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
            throw new UnhandledAppointmentException();
        });

        Assert.assertEquals(2, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());
        Assert.assertEquals(payload, read.get(1).getPayload());
    }

    @org.junit.Test
    public void test_multiple_book_and_handle() throws Exception {
        String topic = "prueba";
        List<String> payloads = IntStream.range(0, 19).mapToObj(i -> String.valueOf(i)).collect(Collectors.toList());
        TestDataSource dataSource = new TestDataSource(
                pg.getEmbeddedPostgres().getPostgresDatabase());

        simpleBookAppointment(topic, payloads, dataSource);

        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                1,
                10,
                new Clock() {
                });

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(10, read.size());
        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(19, read.size());
        //Check order is respected
        for (int i = 0; i < read.size(); i++) {
            Assert.assertEquals(String.valueOf(i), read.get(i).getPayload());
        }
    }

    @org.junit.Test
    public void test_delayed_book_and_handle() throws Exception {
        String topic = "prueba";
        TestDataSource dataSource = new TestDataSource(
                pg.getEmbeddedPostgres().getPostgresDatabase());

        AtomicLong time = new AtomicLong(0L);

        Clock clock = new Clock() {
            @Override
            public long nowEpochMs() {
                return time.get();
            }
        };

        AjendaBooker booker = new AjendaBooker(
                dataSource,
                topic,
                clock);

        booker.bookAppointment(
                AppointmentBookingBuilder.aBooking()
                        .withDueTimestamp(1)
                        .build());

        booker.bookAppointment(
                AppointmentBookingBuilder.aBooking()
                        .withDueTimestamp(2)
                        .build());

        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                clock);

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(0, read.size());

        time.set(2L);

        scheduler.checkAgenda().once(10).readAtMostOnce(false, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(2, read.size());
    }

    private void simpleBookAppointment(String topic, String payload, TestDataSource dataSource) throws Exception {
        AjendaBooker booker = new AjendaBooker(
                dataSource,
                topic,
                new Clock() {
                });

        booker.bookAppointment(
                AppointmentBookingBuilder.aBooking()
                        .withPayload(payload)
                        .build());
    }

    private void simpleBookAppointment(String topic, List<String> payloads, TestDataSource dataSource) throws Exception {
        AjendaBooker booker = new AjendaBooker(
                dataSource,
                topic,
                new Clock() {
                });

        for (String payload : payloads) {
            booker.bookAppointment(
                    AppointmentBookingBuilder.aBooking()
                            .withPayload(payload)
                            .build());
        }
    }


}
