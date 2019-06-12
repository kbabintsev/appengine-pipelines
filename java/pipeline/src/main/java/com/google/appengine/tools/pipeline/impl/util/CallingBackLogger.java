package com.google.appengine.tools.pipeline.impl.util;

import com.google.common.base.Throwables;
import org.slf4j.event.Level;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

import java.util.Optional;
import java.util.function.Consumer;

public final class CallingBackLogger extends MarkerIgnoringBase {
    private final Consumer<String> lineConsumer;

    public CallingBackLogger(final Consumer<String> lineConsumer) {
        this.lineConsumer = lineConsumer;
    }

    public boolean isTraceEnabled() {
        return true;
    }

    public void trace(final String msg) {
        this.log(Level.TRACE, msg, (Throwable) null);
    }

    public void trace(final String format, final Object arg) {
        final FormattingTuple ft = MessageFormatter.format(format, arg);
        this.log(Level.TRACE, ft.getMessage(), ft.getThrowable());
    }

    public void trace(final String format, final Object arg1, final Object arg2) {
        final FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
        this.log(Level.TRACE, ft.getMessage(), ft.getThrowable());
    }

    public void trace(final String format, final Object... argArray) {
        final FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
        this.log(Level.TRACE, ft.getMessage(), ft.getThrowable());
    }

    public void trace(final String msg, final Throwable t) {
        this.log(Level.TRACE, msg, t);
    }

    public boolean isDebugEnabled() {
        return true;
    }

    public void debug(final String msg) {
        this.log(Level.DEBUG, msg, (Throwable) null);
    }

    public void debug(final String format, final Object arg) {
        final FormattingTuple ft = MessageFormatter.format(format, arg);
        this.log(Level.DEBUG, ft.getMessage(), ft.getThrowable());
    }

    public void debug(final String format, final Object arg1, final Object arg2) {
        final FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
        this.log(Level.DEBUG, ft.getMessage(), ft.getThrowable());
    }

    public void debug(final String format, final Object... argArray) {
        final FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
        this.log(Level.DEBUG, ft.getMessage(), ft.getThrowable());
    }

    public void debug(final String msg, final Throwable t) {
        this.log(Level.DEBUG, msg, t);
    }

    public boolean isInfoEnabled() {
        return true;
    }

    public void info(final String msg) {
        this.log(Level.INFO, msg, (Throwable) null);
    }

    public void info(final String format, final Object arg) {
        final FormattingTuple ft = MessageFormatter.format(format, arg);
        this.log(Level.INFO, ft.getMessage(), ft.getThrowable());
    }

    public void info(final String format, final Object arg1, final Object arg2) {
        final FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
        this.log(Level.INFO, ft.getMessage(), ft.getThrowable());
    }

    public void info(final String format, final Object... argArray) {
        final FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
        this.log(Level.INFO, ft.getMessage(), ft.getThrowable());
    }

    public void info(final String msg, final Throwable t) {
        this.log(Level.INFO, msg, t);
    }

    public boolean isWarnEnabled() {
        return true;
    }

    public void warn(final String msg) {
        this.log(Level.WARN, msg, (Throwable) null);
    }

    public void warn(final String format, final Object arg) {
        final FormattingTuple ft = MessageFormatter.format(format, arg);
        this.log(Level.WARN, ft.getMessage(), ft.getThrowable());
    }

    public void warn(final String format, final Object arg1, final Object arg2) {
        final FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
        this.log(Level.WARN, ft.getMessage(), ft.getThrowable());
    }

    public void warn(final String format, final Object... argArray) {
        final FormattingTuple ft = MessageFormatter.arrayFormat(format, argArray);
        this.log(Level.WARN, ft.getMessage(), ft.getThrowable());
    }

    public void warn(final String msg, final Throwable t) {
        this.log(Level.WARN, msg, t);
    }

    public boolean isErrorEnabled() {
        return true;
    }

    public void error(final String msg) {
        this.log(Level.ERROR, msg, (Throwable) null);
    }

    public void error(final String format, final Object arg) {
        final FormattingTuple ft = MessageFormatter.format(format, arg);
        this.log(Level.ERROR, ft.getMessage(), ft.getThrowable());
    }

    public void error(final String format, final Object arg1, final Object arg2) {
        final FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
        this.log(Level.ERROR, ft.getMessage(), ft.getThrowable());
    }

    public void error(final String format, final Object... arguments) {
        final FormattingTuple ft = MessageFormatter.arrayFormat(format, arguments);
        this.log(Level.ERROR, ft.getMessage(), ft.getThrowable());
    }

    public void error(final String msg, final Throwable t) {
        this.log(Level.ERROR, msg, t);
    }

    private void log(final Level level, final String msg, final Throwable t) {
        lineConsumer.accept(
                level.toString()
                        + " " + msg
                        + Optional.ofNullable(t).map(Throwables::getStackTraceAsString).map(out -> " " + out).orElse("")
        );
    }
}
