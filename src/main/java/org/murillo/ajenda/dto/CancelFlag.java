package org.murillo.ajenda.dto;

public class CancelFlag {

    private volatile boolean cancelled = false;

    public boolean isCancelled() {
        return cancelled;
    }

    public void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
