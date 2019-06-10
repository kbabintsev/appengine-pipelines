package com.google.appengine.tools.pipeline;

import com.google.inject.Injector;

public final class PipelineInjector {

    private static Injector injector;

    private PipelineInjector() {
    }

    public static Injector getInjector() {
        if (injector == null) {
            throw new IllegalStateException("Injector haven't been set yet. Please configure framework correctly");
        }
        return injector;
    }

    public static void setInjector(final Injector injector) {
        PipelineInjector.injector = injector;
    }
}
