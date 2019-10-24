package com.google.appengine.tools.pipeline;

public class Retry extends RuntimeException {
    public Retry() {
    }

    public Retry(final String message) {
        super(message);
    }
}
