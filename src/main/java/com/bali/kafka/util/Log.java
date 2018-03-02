package com.bali.kafka.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.bali.kafka.util.ContextualInfo.*;

/**
 * Created by bsing10 on 10/7/15.
 */
public final class Log {

    private final Logger log;

    public Log(Logger log) {
        this.log = log;
    }

    public static Log getLogger(String name) {
        return new Log(LoggerFactory.getLogger(name));
    }

    public static Log getLogger(Class clazz) {
        return new Log(LoggerFactory.getLogger(clazz));
    }

    public void trace(String format, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(getLogContext().getPrefix() + format, args);
        }
    }

    public void trace(String format, Throwable t, Object... args) {
        if (log.isTraceEnabled()) {
            log.trace(getLogContext().getPrefix() + format, args,t);
        }
    }

    public void debug(String format, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(getLogContext().getPrefix() + format, args);
        }
    }

    public void debug(String format, Throwable t, Object... args) {
        if (log.isDebugEnabled()) {
            log.debug(getLogContext().getPrefix() + format, args,t);
        }
    }

    public void error(String format, Object... args) {
        log.error(getLogContext().getPrefix() + format, args);
    }

    public void error(String format, Throwable t, Object... args) {
        log.error(getLogContext().getPrefix() + format, args,t);
    }

    public void fatal(String format, Object... args) {
        log.error(getLogContext().getPrefix() + format, args);
    }

    public void fatal(String format, Throwable t, Object... args) {
        log.error(getLogContext().getPrefix() + format, args,t);
    }

    public void info(String format, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(getLogContext().getPrefix() + format, args);
        }
    }

    public void info(String format, Throwable t, Object... args) {
        if (log.isInfoEnabled()) {
            log.info(getLogContext().getPrefix() + format, args,t);
        }
    }

    public void warn(String format, Object... args) {
        log.warn(getLogContext().getPrefix() + format, args);
    }

    public void warn(String format, Throwable t, Object... args) {
        log.warn(getLogContext().getPrefix() + format, args,t);
    }

    public Logger getInnerLogger() {
        return log;
    }
}