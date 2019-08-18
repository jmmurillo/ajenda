package org.murillo.ajenda.dto;

public enum PeriodicPatternType {
    FIXED_RATE(1),
    FIXED_DELAY(2),
    CRON_UNIX(10),
    CRON_CRON4J(11),
    CRON_QUARTZ(12),
    CRON_SPRING(13);

    private int id;

    public int getId() {
        return id;
    }

    PeriodicPatternType(int id) {
        this.id = id;
    }

    public static PeriodicPatternType fromId(int id) {
        switch (id) {
            case 1:
                return FIXED_RATE;
            case 2:
                return FIXED_DELAY;
            case 10:
                return CRON_UNIX;
            case 11:
                return CRON_CRON4J;
            case 12:
                return CRON_QUARTZ;
            case 13:
                return CRON_SPRING;
            default:
                return null;
        }
    }


}
