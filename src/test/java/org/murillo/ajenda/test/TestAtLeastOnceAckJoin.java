package org.murillo.ajenda.test;

import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.Assert;
import org.junit.ClassRule;
import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.common.Clock;
import org.murillo.ajenda.dto.AppointmentBookingBuilder;
import org.murillo.ajenda.dto.AppointmentDue;
import org.murillo.ajenda.handling.AjendaScheduler;
import org.murillo.ajenda.test.utils.TestDataSource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestAtLeastOnceAckJoin {

    @ClassRule
    public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

    @org.junit.Test
    public void test_simple_book_and_handle_at_most_once() throws Exception {
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
    }

    @org.junit.Test
    public void test_multiple_book_and_handle_at_most_once() throws Exception {
        String topic = "prueba";
        List<String> payloads = IntStream.range(0, 19).mapToObj(i -> UUID.randomUUID().toString()).collect(Collectors.toList());
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
    }

    @org.junit.Test
    public void test_simple_book_and_handle_at_least_once_each() throws Exception {
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
        scheduler.checkAgenda().once(10).readAtLeastOnceAckEach(10000L, appointmentDue -> {
            read.add(appointmentDue);
        });

        Assert.assertEquals(1, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());
    }

    @org.junit.Test
    public void test_simple_book_and_handle_at_least_once_join() throws Exception {
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
        scheduler.checkAgenda().once(10).readAtLeastOnceAckJoin(10000L, appointmentDue -> {
            read.add(appointmentDue);
        });

        Assert.assertEquals(1, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());
    }

    @org.junit.Test
    public void test_simple_book_and_handle_at_least_once_atomic() throws Exception {
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
        scheduler.checkAgenda().once(10).readAtLeastOnceAtomic(appointmentDue -> {
            read.add(appointmentDue);
        });

        Assert.assertEquals(1, read.size());
        Assert.assertEquals(payload, read.get(0).getPayload());
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
