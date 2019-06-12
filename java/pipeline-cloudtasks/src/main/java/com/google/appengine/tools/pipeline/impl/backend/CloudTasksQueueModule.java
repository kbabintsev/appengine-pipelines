package com.google.appengine.tools.pipeline.impl.backend;

import com.cloudaware.deferred.DeferredTaskContext;
import com.cloudaware.deferred.DeferredTaskServlet;
import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;

public class CloudTasksQueueModule extends ServletModule {

    @Override
    protected void configureServlets() {
        bind(PipelineTaskQueue.class).to(CloudTasksQueue.class);
        bind(DeferredTaskServlet.class).in(Singleton.class);
        serve(DeferredTaskContext.DEFAULT_DEFERRED_URL).with(DeferredTaskServlet.class);
        filter(DeferredTaskContext.DEFAULT_DEFERRED_URL).through(PipelineTaskQueueInjectFilter.class);
    }
}
