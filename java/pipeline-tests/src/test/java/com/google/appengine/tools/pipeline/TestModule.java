package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueueModule;
import com.google.inject.AbstractModule;

public final class TestModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new AppEngineTaskQueueModule());
        install(new PipelineModule());
    }
}
