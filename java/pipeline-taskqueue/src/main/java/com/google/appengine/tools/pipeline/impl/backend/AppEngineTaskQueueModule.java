package com.google.appengine.tools.pipeline.impl.backend;

import com.google.inject.AbstractModule;

public class AppEngineTaskQueueModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(PipelineTaskQueue.class).to(AppEngineTaskQueue.class);
    }
}
