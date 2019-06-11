package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineServiceImpl;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.inject.servlet.ServletModule;

public class PipelineModule extends ServletModule {
    @Override
    protected void configureServlets() {
        bind(PipelineService.class).to(PipelineServiceImpl.class);
        bind(PipelineBackEnd.class).to(AppEngineBackEnd.class);
        serve(PipelineServlet.baseUrl() + "*").with(PipelineServlet.class);
    }
}
