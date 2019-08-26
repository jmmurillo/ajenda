package org.murillo.ajenda.dto;

public interface Clock {

    default long nowEpochMs(){return System.currentTimeMillis();};

    default boolean shutdown(long gracePeriod){return true;}

}
