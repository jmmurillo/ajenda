package org.murillo.ajenda;

public class CancelFlag {
    
    private volatile boolean cancelled = false;

    public boolean isCancelled() {
        return cancelled;
    }

    void setCancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
