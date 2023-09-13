//package com.google.appengine.tools.pipeline.impl.backend;
//
//import com.google.appengine.api.taskqueue.DeferredTaskContext;
//import com.google.inject.servlet.ServletModule;
//
//public class AppEngineTaskQueueModule extends ServletModule {
//
//    @Override
//    protected void configureServlets() {
//        bind(PipelineTaskQueue.class).to(AppEngineTaskQueue.class);
//        filter(DeferredTaskContext.DEFAULT_DEFERRED_URL).through(PipelineTaskQueueInjectFilter.class);
//    }
//}
