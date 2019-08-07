package org.murillo.ajenda.common;

public interface Clock {

    default long nowEpochMs(){return System.currentTimeMillis();};

    default boolean shutdown(long gracePeriod){return true;}

}
