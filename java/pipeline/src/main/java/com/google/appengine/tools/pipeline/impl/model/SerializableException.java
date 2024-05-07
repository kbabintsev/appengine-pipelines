package com.google.appengine.tools.pipeline.impl.model;

public final class SerializableException extends Exception {
    private final Class<? extends Throwable> originalExceptionClass;

    public SerializableException(final Throwable nonSerializabbleException) {
        super(
                "Original non-serializable exception follows:\n" + nonSerializabbleException.toString()
        );
        setStackTrace(nonSerializabbleException.getStackTrace());
        this.originalExceptionClass = nonSerializabbleException.getClass();
    }

    public Class<? extends Throwable> getOriginalExceptionClass() {
        return originalExceptionClass;
    }
}
