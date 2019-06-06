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
import com.google.appengine.tools.pipeline.PipelineService;

import java.util.Set;
import java.util.UUID;

/**
 * Implements {@link PipelineService} by delegating to {@link PipelineManager}.
 * 
 * @author rudominer@google.com (Mitch Rudominer)
 * 
 */
public class PipelineServiceImpl implements PipelineService {


  @Override
  public UUID startNewPipeline(Job0<?> jobInstance, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance);
  }

  @Override
  public <T1> UUID startNewPipeline(Job1<?, T1> jobInstance, T1 arg1, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1);
  }

  @Override
  public <T1, T2> UUID startNewPipeline(Job2<?, T1, T2> jobInstance, T1 arg1, T2 arg2,
      JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2);
  }

  @Override
  public <T1, T2, T3> UUID startNewPipeline(Job3<?, T1, T2, T3> jobInstance, T1 arg1, T2 arg2,
      T3 arg3, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3);
  }

  @Override
  public <T1, T2, T3, T4> UUID startNewPipeline(Job4<?, T1, T2, T3, T4> jobInstance, T1 arg1,
      T2 arg2, T3 arg3, T4 arg4, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4);
  }

  @Override
  public <T1, T2, T3, T4, T5> UUID startNewPipeline(Job5<?, T1, T2, T3, T4, T5> jobInstance,
      T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5);
  }

  @Override
  public <T1, T2, T3, T4, T5, T6> UUID startNewPipeline(
      Job6<?, T1, T2, T3, T4, T5, T6> jobInstance, T1 arg1, T2 arg2, T3 arg3, T4 arg4, T5 arg5,
      T6 arg6, JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arg1, arg2, arg3, arg4, arg5,
        arg6);
  }

  @Override
  public UUID startNewPipelineUnchecked(Job<?> jobInstance, Object[] arguments,
      JobSetting... settings) {
    return PipelineManager.startNewPipeline(settings, jobInstance, arguments);
  }

  @Override
  public void stopPipeline(UUID jobHandle) throws NoSuchObjectException {
    PipelineManager.stopJob(jobHandle);
  }
  
  @Override
  public void cancelPipeline(UUID jobHandle) throws NoSuchObjectException {
    PipelineManager.cancelJob(jobHandle);
  }

  @Override
  public void deletePipelineRecords(UUID pipelineHandle) throws NoSuchObjectException,
      IllegalStateException {
    deletePipelineRecords(pipelineHandle, false, false);
  }

  @Override
  public void deletePipelineRecords(UUID pipelineHandle, boolean force, boolean async)
      throws NoSuchObjectException, IllegalStateException {
    PipelineManager.deletePipelineRecords(pipelineHandle, force, async);
  }

  @Override
  public JobInfo getJobInfo(UUID jobHandle) throws NoSuchObjectException {
    return PipelineManager.getJob(jobHandle);
  }

  @Override
  public void submitPromisedValue(UUID promiseHandle, Object value)
      throws NoSuchObjectException, OrphanedObjectException {
    PipelineManager.acceptPromisedValue(promiseHandle, value);
  }

  @Override
  public void cleanBobs(final String prefix) {
    PipelineManager.getBackEnd().cleanBlobs(prefix);
  }

  @Override
  public void shutdown() {
    PipelineManager.shutdown();
  }

  @Override
  public Set<UUID> getTestPipelines() {
    return PipelineManager.getBackEnd().getTestPipelines();
  }
}
