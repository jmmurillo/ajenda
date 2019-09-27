package org.murillo.ajenda.dto;

public enum PeriodicPatternType {
    FIXED_RATE(1, false),
    FIXED_DELAY(2, false),
    CRON_UNIX(10, true),
    CRON_CRON4J(11, true),
    CRON_QUARTZ(12, true),
    CRON_SPRING(13, true);

    private int id;
    private boolean isCron;

    public int getId() {
        return id;
    }

    public boolean isCron() {
        return isCron;
    }

    PeriodicPatternType(int id, boolean isCron) {
        this.id = id;
        this.isCron = isCron;
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
