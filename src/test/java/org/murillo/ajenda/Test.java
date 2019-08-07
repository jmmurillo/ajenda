package org.murillo.ajenda;

import org.murillo.ajenda.booking.AjendaBooker;
import org.murillo.ajenda.common.SyncedClock;
import org.murillo.ajenda.dto.AppointmentBookingBuilder;
import org.murillo.ajenda.handling.AjendaScheduler;

import java.time.Instant;

public class Test {


    @org.junit.Test
    public void testTime() throws Exception {
        HikariCPDataSource dataSource = new HikariCPDataSource();
        SyncedClock timeProvider = new SyncedClock(dataSource, 10);
    }

    @org.junit.Test
    public void test() throws Exception {

        System.out.println(formatTimestamp(Instant.now().toEpochMilli()));
        String topic = "prueba";
        HikariCPDataSource dataSource = new HikariCPDataSource();

        AjendaBooker booker = new AjendaBooker(dataSource, topic);

        AjendaScheduler scheduler1 = new AjendaScheduler(dataSource, topic);
        scheduler1.checkAgenda()
                .withCustomSqlCondition("MOD(\"due_date\",2) = 1")
                .periodically(5, 2000).readAtLeastOnceAckJoin(
                15000,
                false,
                a -> {
                    System.out.println(a);
                    System.out.println("1 Retraso: " + (Instant.now().toEpochMilli() - a.getDueTimestamp()) + " ; Payload: " + a.getPayload());
                });

        System.out.println("Enviando mensaje a " + formatTimestamp());
        for (int i = 0; i < 1000; i++) {
            int delayMs = 501 * i;
            int finalI = i;
            booker.bookAppointment(
                    AppointmentBookingBuilder.aBooking()
                            .withDelayedDue(delayMs)
                            .withPayload(String.valueOf(i))
                            .withHashUid()
                            .withExtraParam("nuevaLong", 1234L + finalI)
                            .withExtraParam("nuevaString", "olakease " + finalI)
                            .build()
            );
        }

        scheduler1.checkAgenda()
                .withCustomSqlCondition("MOD(\"due_date\",2) = 0")
                .periodically(5, 2000).readAtLeastOnceAckJoin(
                15000,
                false,
                a -> {
                    System.out.println(a);
                    System.out.println("1 Retraso: " + (Instant.now().toEpochMilli() - a.getDueTimestamp()) + " ; Payload: " + a.getPayload());
                });

        Thread.sleep(600000);

    }

    private static String formatTimestamp(long timestamp) {
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return Instant.ofEpochMilli(timestamp).toString();
    }

    private static String formatTimestamp() {
        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        return Instant.now().toString();
    }
}
