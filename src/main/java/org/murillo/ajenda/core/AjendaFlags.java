package org.murillo.ajenda.core;

public class AjendaFlags {
    
    public static final int GEN_NEXT_FLAG = 1 << 0;
    public static final int SKIP_MISSED_FLAG = 1 << 1;
    
    public static boolean isGenNext(int flags){
        return (flags & GEN_NEXT_FLAG) != 0;
    }

    public static boolean isSkipMissed(int flags){
        return (flags & SKIP_MISSED_FLAG) != 0;
    }
    
    public static int withFlags(int... flags){
        if(flags == null) return 0;
        int result = 0;
        for(int f : flags) result |= f;
        return result;
    }

    public static int removeFlags(int flags, int... toRemove){
        int result = flags;
        for(int f : toRemove) result &= ~f;
        return result;
    }

    public static int addFlags(int flags, int... toAdd){
        int result = flags;
        for(int f : toAdd) result |= f;
        return result;
    }
    
}
