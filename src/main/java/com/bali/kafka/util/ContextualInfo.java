package com.bali.kafka.util;

/**
 * Provide log context to each thread to provide extra log info with each log call.
 */
public final class ContextualInfo {
    private String prefix = "[" + Thread.currentThread().getName() + "]";

    public static ThreadLocal<ContextualInfo> CONTEXT = ThreadLocal.withInitial(() -> new ContextualInfo());

    public static ContextualInfo getLogContext() {
        return CONTEXT.get();
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}