package org.murillo.ajenda.test;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.murillo.ajenda.AjendaScheduler;
import org.murillo.ajenda.dto.*;
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

public class TestAtLeastOnce {

    @ClassRule
    public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

    public TestDataSource dataSource;

    @Before
    public void clearDB() throws SQLException {
        dataSource = new TestDataSource(pg.getEmbeddedPostgres().getPostgresDatabase());
        try (Connection connection = dataSource.getConnection()) {
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
        
        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                new Clock() {
                });
        
        simpleBookAppointment(scheduler, payload);

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
            read.add(appointmentDue);
        });

        Assert.assertEquals(1, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());

        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(1, read.size());
    }


    @org.junit.Test
    public void test_multiple_book_and_handle() throws Exception {
        String topic = "prueba";
        List<String> payloads = IntStream.range(0, 19).mapToObj(i -> String.valueOf(i)).collect(Collectors.toList());


        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                1,
                10,
                new Clock() {
                });

        simpleBookAppointment(scheduler, payloads);
        
        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(10, read.size());
        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
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

        AtomicLong time = new AtomicLong(0L);

        Clock clock = new Clock() {
            @Override
            public long nowEpochMs() {
                return time.get();
            }
        };



        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                clock);

        scheduler.book(
                AppointmentBookingBuilder.aBooking()
                        .withDueTimestamp(1)
                        .build());

        scheduler.book(
                AppointmentBookingBuilder.aBooking()
                        .withDueTimestamp(2)
                        .build());

        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(0, read.size());

        time.set(2L);

        scheduler.checkAgenda().withFetchSize(10).once().readAtLeastOnce(10000, appointmentDue -> {
            read.add(appointmentDue);
        });
        Assert.assertEquals(2, read.size());
    }

    @org.junit.Test
    public void test_periodic_appointment() throws Exception {
        String topic = "prueba";
        String payload = UUID.randomUUID().toString();

        AjendaScheduler scheduler = new AjendaScheduler(
                dataSource,
                topic,
                new Clock() {
                });

        long t = System.currentTimeMillis();
        ArrayList<AppointmentDue> read = new ArrayList<>();
        scheduler.checkAgenda().withFetchSize(10).periodically(100)
                .readAtLeastOnce(10000, e -> {
                    read.add(e);
                });

        scheduler.bookPeriodic(
                PeriodicAppointmentBookingBuilder.aPeriodicBooking()
                        .withFixedPeriod(500, PeriodicPatternType.FIXED_RATE)
                        .withPayload(payload)
                        .build());

        Thread.sleep(4750);
        scheduler.shutdown(0);

        Assert.assertEquals(10, read.size());

    }

    private void simpleBookAppointment(AjendaBooker booker, String payload) throws Exception {
        booker.book(
                AppointmentBookingBuilder.aBooking()
                        .withPayload(payload)
                        .build());
    }

    private void simpleBookAppointment(AjendaBooker booker, List<String> payloads) throws Exception {
        for (String payload : payloads) {
            booker.book(
                    AppointmentBookingBuilder.aBooking()
                            .withPayload(payload)
                            .build());
        }
    }
}
