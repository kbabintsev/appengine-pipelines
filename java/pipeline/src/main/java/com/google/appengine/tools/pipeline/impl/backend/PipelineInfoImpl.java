package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.PipelineInfo;
import com.google.appengine.tools.pipeline.impl.model.PipelineRecord;

public class PipelineInfoImpl implements PipelineInfo {
    private final PipelineRecord pipeline;

    public PipelineInfoImpl(final PipelineRecord pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public String getPipelineDisplayName() {
        return pipeline.getPipelineDisplayName();
    }

    @Override
    public State getJobState() {
        return pipeline.getRootJob().getJobState();
    }

    @Override
    public Object getOutput() {
        return pipeline.getRootJob().getOutput();
    }

    @Override
    public String getError() {
        return pipeline.getRootJob().getError();
    }

    @Override
    public Throwable getException() {
        return pipeline.getRootJob().getException();
    }
}
