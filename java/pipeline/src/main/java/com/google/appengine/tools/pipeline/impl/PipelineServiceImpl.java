// Copyright 2011 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.Job3;
import com.google.appengine.tools.pipeline.Job4;
import com.google.appengine.tools.pipeline.Job5;
import com.google.appengine.tools.pipeline.Job6;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineInfo;
import com.google.appengine.tools.pipeline.PipelineService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;
import java.util.UUID;

/**
 * Implements {@link PipelineService} by delegating to {@link PipelineManager}.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
public class PipelineServiceImpl implements PipelineService {

    private final PipelineManager pipelineManager;

    @Inject
    public PipelineServiceImpl(final PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
    }

    @Override
    public UUID startNewPipeline(final Job0<?> jobInstance, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance);
    }

    @Override
    public <T1> UUID startNewPipeline(final Job1<?, T1> jobInstance, final T1 arg1, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1);
    }

    @Override
    public <T1, T2> UUID startNewPipeline(final Job2<?, T1, T2> jobInstance, final T1 arg1, final T2 arg2,
                                          final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2);
    }

    @Override
    public <T1, T2, T3> UUID startNewPipeline(final Job3<?, T1, T2, T3> jobInstance, final T1 arg1, final T2 arg2,
                                              final T3 arg3, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3);
    }

    @Override
    public <T1, T2, T3, T4> UUID startNewPipeline(final Job4<?, T1, T2, T3, T4> jobInstance, final T1 arg1,
                                                  final T2 arg2, final T3 arg3, final T4 arg4, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4);
    }

    @Override
    public <T1, T2, T3, T4, T5> UUID startNewPipeline(final Job5<?, T1, T2, T3, T4, T5> jobInstance,
                                                      final T1 arg1, final T2 arg2, final T3 arg3, final T4 arg4, final T5 arg5, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5);
    }

    @Override
    public <T1, T2, T3, T4, T5, T6> UUID startNewPipeline(
            final Job6<?, T1, T2, T3, T4, T5, T6> jobInstance, final T1 arg1, final T2 arg2, final T3 arg3, final T4 arg4, final T5 arg5,
            final T6 arg6, final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5,
                arg6);
    }

    @Override
    public UUID startNewPipelineUnchecked(final Job<?> jobInstance, final Object[] arguments,
                                          final JobSetting... settings) {
        return pipelineManager.startNewPipeline(settings, jobInstance, arguments);
    }

    @Override
    public void stopPipeline(final UUID pipelineKey) throws NoSuchObjectException {
        pipelineManager.stopJob(pipelineKey, pipelineKey);
    }

    @Override
    public void cancelPipeline(final UUID pipelineKey) throws NoSuchObjectException {
        pipelineManager.cancelJob(pipelineKey, pipelineKey);
    }

    @Override
    public void deletePipelineRecords(final UUID pipelineKey) throws NoSuchObjectException,
            IllegalStateException {
        deletePipelineRecords(pipelineKey, false, false);
    }

    @Override
    public void deletePipelineRecords(final UUID pipelineKey, final boolean force, final boolean async)
            throws NoSuchObjectException, IllegalStateException {
        pipelineManager.deletePipelineRecords(pipelineKey, force, async);
    }

    @Override
    public JobInfo getJobInfo(final UUID pipelineKey, final UUID jobKey) throws NoSuchObjectException {
        return pipelineManager.getJob(pipelineKey, jobKey);
    }

    @Override
    public PipelineInfo getPipelineInfo(final UUID pipelineKey) throws NoSuchObjectException {
        return pipelineManager.getPipeline(pipelineKey);
    }

    @Override
    public void submitPromisedValue(final UUID pipelineKey, final UUID promiseKey, final Object value)
            throws NoSuchObjectException, OrphanedObjectException {
        pipelineManager.acceptPromisedValue(pipelineKey, promiseKey, value);
    }

    @Override
    public void cleanBobs(final String prefix) {
        pipelineManager.getBackEnd().cleanBlobs(prefix);
    }

    @Override
    public void shutdown() {
        pipelineManager.shutdown();
    }

    @Override
    public Set<UUID> getTestPipelines() {
        return pipelineManager.getBackEnd().getTestPipelines();
    }
}
