package com.google.appengine.tools.pipeline.impl.backend;

import com.cloudaware.deferred.DeferredTask;
import com.cloudaware.deferred.DeferredTaskContext;

import javax.servlet.http.HttpServletRequest;

class CloudTasksDeferredTask implements DeferredTask {
    private static final long serialVersionUID = 7439951375557875977L;
    private final PipelineTaskQueue.Deferred deferred;

    CloudTasksDeferredTask(final PipelineTaskQueue.Deferred deferred) {
        this.deferred = deferred;
    }

    @Override
    public void run(final HttpServletRequest request) {
        deferred.run(request, new PipelineTaskQueue.DeferredContext() {
            @Override
            public void markForRetry() {
                DeferredTaskContext.markForRetry(request);
            }
        });
    }
}
