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

package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Job's state persistence.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class JobInstanceRecord extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "JobInstanceRecord";
  private static final String JOB_KEY_PROPERTY = "jobKey";
  private static final String JOB_CLASS_NAME_PROPERTY = "jobClassName";
  public static final String JOB_DISPLAY_NAME_PROPERTY = "jobDisplayName";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    JOB_KEY_PROPERTY,
                    JOB_CLASS_NAME_PROPERTY,
                    JOB_DISPLAY_NAME_PROPERTY
            )
            .build();

  // persistent
  private final UUID jobKey;
  private final String jobClassName;
  private final String jobDisplayName;
  private final byte[] value;

  // transient
  private Job<?> jobInstance;

  public JobInstanceRecord(JobRecord job, Job<?> jobInstance) {
    super(DATA_STORE_KIND, job.getRootJobKey(), job.getGeneratorJobKey(), job.getGraphGuid());
    jobKey = job.getKey();
    jobClassName = jobInstance.getClass().getName();
    jobDisplayName = jobInstance.getJobDisplayName();
    try {
        value = PipelineManager.getBackEnd().serializeValue(this, jobInstance);
    } catch (IOException e) {
      throw new RuntimeException("Exception while attempting to serialize the jobInstance "
          + jobInstance, e);
    }
 }

  public JobInstanceRecord(StructReader entity) {
    super(DATA_STORE_KIND, entity);
    jobKey = UUID.fromString(entity.getString(JOB_KEY_PROPERTY)); // probably not null?
    jobClassName = entity.getString(JOB_CLASS_NAME_PROPERTY); // probably not null?
    if (!entity.isNull(JOB_DISPLAY_NAME_PROPERTY)) {
      jobDisplayName = entity.getString(JOB_DISPLAY_NAME_PROPERTY);
    } else {
      jobDisplayName = jobClassName;
    }

    value = PipelineManager.getBackEnd().retrieveBlob(getRootJobKey(), getKey());
  }

  @Override
  public PipelineMutation toEntity() {
    PipelineMutation mutation = toProtoEntity();
    final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
    entity.set(JOB_KEY_PROPERTY).to(jobKey.toString());
    entity.set(JOB_CLASS_NAME_PROPERTY).to(jobClassName);
    entity.set(JOB_DISPLAY_NAME_PROPERTY).to(jobDisplayName);
    mutation.setBlobMutation(new PipelineMutation.BlobMutation(getRootJobKey(), getKey(), value));
    return mutation;
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public UUID getJobKey() {
    return jobKey;
  }

  /**
   * Returns the job class name for display purpose only.
   */
  public String getJobDisplayName() {
    return jobDisplayName;
  }

  public String getClassName() {
    return jobClassName;
  }

  public synchronized Job<?> getJobInstanceDeserialized() {
    if (null == jobInstance) {
      try {
        jobInstance = (Job<?>) PipelineManager.getBackEnd().deserializeValue(this, value);
      } catch (IOException e) {
        throw new RuntimeException(
            "Exception while attempting to deserialize jobInstance for " + jobKey, e);
      }
    }
    return jobInstance;
  }
}
