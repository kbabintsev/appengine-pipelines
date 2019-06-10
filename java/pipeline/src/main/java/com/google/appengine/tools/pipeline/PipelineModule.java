package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.inject.AbstractModule;

public class PipelineModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(PipelineBackEnd.class).to(AppEngineBackEnd.class);
    }
}
