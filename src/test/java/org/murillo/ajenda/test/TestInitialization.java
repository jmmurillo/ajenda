//package org.murillo.ajenda.test;
//
//import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
//import io.zonky.test.db.postgres.junit.SingleInstancePostgresRule;
//import org.hamcrest.CoreMatchers;
//import org.junit.Assert;
//import org.junit.Rule;
//import org.murillo.ajenda.dto.AjendaBooker;
//import org.murillo.ajenda.dto.Clock;
//import org.murillo.ajenda.test.utils.TestDataSource;
//import org.murillo.ajenda.test.utils.TestUtils;
//import org.murillo.ajenda.Common;
//
//import java.sql.Connection;
//import java.sql.Statement;
//import java.time.Instant;
//
//public class TestInitialization {
//
//    @Rule
//    public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();
//
//    @org.junit.Test
//    public void test_table_initialization_creates() throws Exception {
//        String topic = "prueba";
//        String tableName = Common.getTableNameForTopic(topic);
//        TestDataSource dataSource = new TestDataSource(
//                pg.getEmbeddedPostgres().getPostgresDatabase());
//
//        Assert.assertFalse(TestUtils.doesTableExist(dataSource, tableName));
//
//        AjendaBooker booker = new AjendaBooker(
//                dataSource,
//                topic,
//                new Clock() {
//                });
//
//        Assert.assertTrue(TestUtils.doesTableExist(dataSource, tableName));
//        Assert.assertThat(
//                TestUtils.getColumnNamesForTable(dataSource, tableName),
//                CoreMatchers.hasItems("uuid", "creation_date", "due_date", "timeout_date", "attempts", "payload"));
//    }
//
//    @org.junit.Test
//    public void test_table_initialization_verifies() throws Exception {
//        String topic = "prueba";
//        String tableName = Common.getTableNameForTopic(topic);
//        TestDataSource dataSource = new TestDataSource(
//                pg.getEmbeddedPostgres().getPostgresDatabase());
//
//        String TABLE_FOR_TOPIC_QUERY =
//                "CREATE TABLE %s ( "
//                        + "id BIGINT PRIMARY KEY);";
//
//        try (Connection conn = dataSource.getConnection()) {
//            String createTableSql = String.format(
//                    TABLE_FOR_TOPIC_QUERY,
//                    tableName);
//            try (Statement stmt = conn.createStatement()) {
//                stmt.execute(createTableSql);
//                conn.commit();
//            }
//        }
//
//        Assert.assertTrue(TestUtils.doesTableExist(dataSource, tableName));
//
//        try {
//            AjendaBooker booker = new AjendaBooker(
//                    dataSource,
//                    topic,
//                    new Clock() {
//                    });
//            Assert.fail("An exception should have been thrown");
//        } catch (IllegalStateException e) {
//            Assert.assertEquals(
//                    "Column uuid does not exist in Ajenda table ajenda_prueba",
//                    e.getMessage()
//            );
//        }
//
//    }
//
//    @org.junit.Test
//    public void test_table_initialization_respects() throws Exception {
//        String topic = "prueba";
//        String tableName = Common.getTableNameForTopic(topic);
//        TestDataSource dataSource = new TestDataSource(
//                pg.getEmbeddedPostgres().getPostgresDatabase());
//
//        String TABLE_FOR_TOPIC_QUERY =
//                "CREATE TABLE %s ( "
//                        + "uuid             UUID PRIMARY KEY, "
//                        + "some_column      TEXT, "
//                        + "creation_date    BIGINT, "
//                        + "due_date         BIGINT, "
//                        + "timeout_date      BIGINT, "
//                        + "attempts         INTEGER, "
//                        + "payload          TEXT,"
//                        + "periodic_uuid             UUID)";
//
//        try (Connection conn = dataSource.getConnection()) {
//            String createTableSql = String.format(
//                    TABLE_FOR_TOPIC_QUERY,
//                    tableName);
//            try (Statement stmt = conn.createStatement()) {
//                stmt.execute(createTableSql);
//                conn.commit();
//            }
//        }
//
//        Assert.assertTrue(TestUtils.doesTableExist(dataSource, tableName));
//
//        AjendaBooker booker = new AjendaBooker(
//                dataSource,
//                topic,
//                new Clock() {
//                });
//
//        Assert.assertThat(
//                TestUtils.getColumnNamesForTable(dataSource, tableName),
//                CoreMatchers.hasItems("some_column"));
//
//    }
//
////    @org.junit.Test
////    public void test() throws Exception {
////
////        System.out.println(formatTimestamp(Instant.now().toEpochMilli()));
////        String topic = "prueba";
////        TestDataSource dataSource = new TestDataSource();
////
////        AjendaBooker booker = new AjendaBooker(dataSource, topic);
////
////        AjendaScheduler scheduler1 = new AjendaScheduler(dataSource, topic);
////        scheduler1.checkAgenda()
////                .withCustomSqlCondition("MOD(\"due_date\",2) = 1")
////                .periodically(5, 2000).readAtLeastOnceAckJoin(
////                15000,
////                false,
////                a -> {
////                    System.out.println(a);
////                    System.out.println("1 Retraso: " + (Instant.now().toEpochMilli() - a.getDueTimestamp()) + " ; Payload: " + a.getPayload());
////                });
////
////        System.out.println("Enviando mensaje a " + formatTimestamp());
////        for (int i = 0; i < 1000; i++) {
////            int delayMs = 501 * i;
////            int finalI = i;
////            booker.bookAppointment(
////                    AppointmentBookingBuilder.aBooking()
////                            .withDelayedDue(delayMs)
////                            .withPayload(String.valueOf(i))
////                            .withHashUid()
////                            .withExtraParam("nuevaLong", 1234L + finalI)
////                            .withExtraParam("nuevaString", "olakease " + finalI)
////                            .build()
////            );
////        }
////
////        scheduler1.checkAgenda()
////                .withCustomSqlCondition("MOD(\"due_date\",2) = 0")
////                .periodically(5, 2000).readAtLeastOnceAckJoin(
////                15000,
////                false,
////                a -> {
////                    System.out.println(a);
////                    System.out.println("1 Retraso: " + (Instant.now().toEpochMilli() - a.getDueTimestamp()) + " ; Payload: " + a.getPayload());
////                });
////
////        Thread.sleep(600000);
////
////    }
//
//    private static String formatTimestamp(long timestamp) {
//        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//        return Instant.ofEpochMilli(timestamp).toString();
//    }
//
//    private static String formatTimestamp() {
//        //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
//        return Instant.now().toString();
//    }
//}
