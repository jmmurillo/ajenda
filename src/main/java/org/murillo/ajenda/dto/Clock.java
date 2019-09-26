package org.murillo.ajenda.dto;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public interface Clock {

    default long nowEpochMs() {
        return System.currentTimeMillis();
    }

    default ZonedDateTime now() {
        return ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(this.nowEpochMs()), ZoneOffset.UTC);
    }

    default boolean shutdown(long gracePeriod) {
        return true;
    }

}
