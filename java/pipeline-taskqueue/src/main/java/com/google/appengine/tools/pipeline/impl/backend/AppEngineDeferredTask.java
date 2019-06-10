package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;

class AppEngineDeferredTask implements DeferredTask {
    private static final long serialVersionUID = -2511005850997921342L;
    private final PipelineTaskQueue.Deferred deferred;

    AppEngineDeferredTask(final PipelineTaskQueue.Deferred deferred) {
        this.deferred = deferred;
    }

    @Override
    public void run() {
        deferred.run(
                DeferredTaskContext.getCurrentRequest(),
                new PipelineTaskQueue.DeferredContext() {
                    @Override
                    public void markForRetry() {
                        DeferredTaskContext.markForRetry();
                    }
                });
    }
}
