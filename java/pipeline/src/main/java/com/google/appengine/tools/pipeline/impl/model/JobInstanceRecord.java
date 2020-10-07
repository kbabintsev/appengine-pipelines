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
import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;
import com.google.appengine.tools.pipeline.impl.util.ValueLocation;
import com.google.appengine.tools.pipeline.impl.util.ValueProxy;
import com.google.appengine.tools.pipeline.impl.util.ValueStoragePath;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Job's state persistence.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class JobInstanceRecord extends PipelineModelObject {

    public static final String DATA_STORE_KIND = "JobInstance";
    public static final int JOB_DISPLAY_NAME_LENGTH = 255;
    public static final String JOB_DISPLAY_NAME_PROPERTY = "jobDisplayName";
    private static final String JOB_KEY_PROPERTY = "jobKey";
    private static final String JOB_CLASS_NAME_PROPERTY = "jobClassName";
    private static final String VALUE_LOCATION_PROPERTY = "valueLocation";
    private static final String DATABASE_VALUE_PROPERTY = "databaseValue";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    JOB_KEY_PROPERTY,
                    JOB_CLASS_NAME_PROPERTY,
                    JOB_DISPLAY_NAME_PROPERTY,
                    VALUE_LOCATION_PROPERTY,
                    DATABASE_VALUE_PROPERTY
            )
            .build();

    // persistent
    private final UUID jobKey;
    private final String jobClassName;
    private final String jobDisplayName;
    private final ValueProxy valueProxy;

    public JobInstanceRecord(final PipelineManager pipelineManager, final JobRecord job, final Job<?> jobInstance) {
        super(DATA_STORE_KIND, job.getPipelineKey(), job.getGeneratorJobKey(), job.getGraphKey());
        jobKey = job.getKey();
        jobClassName = jobInstance.getClass().getName();
        jobDisplayName = jobInstance.getJobDisplayName();
        valueProxy = new ValueProxy(
                pipelineManager,
                jobInstance,
                new ValueStoragePath(getPipelineKey(), DATA_STORE_KIND, getKey())
        );
    }

    public JobInstanceRecord(final PipelineManager pipelineManager, @Nullable final String prefix, final StructReader entity) {
        super(DATA_STORE_KIND, prefix, entity);
        jobKey = UUID.fromString(entity.getString(Record.property(prefix, JOB_KEY_PROPERTY))); // probably not null?
        jobClassName = entity.getString(Record.property(prefix, JOB_CLASS_NAME_PROPERTY)); // probably not null?
        if (!entity.isNull(Record.property(prefix, JOB_DISPLAY_NAME_PROPERTY))) {
            jobDisplayName = entity.getString(Record.property(prefix, JOB_DISPLAY_NAME_PROPERTY));
        } else {
            jobDisplayName = jobClassName;
        }
        final String valueLocationProperty = Record.property(prefix, VALUE_LOCATION_PROPERTY);
        final String databaseValueProperty = Record.property(prefix, DATABASE_VALUE_PROPERTY);
        valueProxy = new ValueProxy(
                pipelineManager,
                entity.isNull(valueLocationProperty) ? ValueLocation.DATABASE : ValueLocation.valueOf(entity.getString(valueLocationProperty)),
                entity.isNull(databaseValueProperty) ? null : entity.getBytes(databaseValueProperty).toByteArray(),
                true,
                new ValueStoragePath(getPipelineKey(), DATA_STORE_KIND, getKey())
        );
    }

    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        entity.set(JOB_KEY_PROPERTY).to(jobKey.toString());
        entity.set(JOB_CLASS_NAME_PROPERTY).to(jobClassName);
        entity.set(JOB_DISPLAY_NAME_PROPERTY).to(StringUtils.abbreviate(jobDisplayName, JOB_DISPLAY_NAME_LENGTH));
        valueProxy.updateStorage(
                location -> entity.set(VALUE_LOCATION_PROPERTY).to(location.name()),
                databaseBlob -> entity.set(DATABASE_VALUE_PROPERTY).to(databaseBlob),
                (storageLocation, storageBlob) -> mutation.setValueMutation(new PipelineMutation.ValueMutation(
                        storageLocation,
                        storageBlob
                ))
        );
        return mutation;
    }

    @Override
    public String getDatastoreKind() {
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
        return (Job<?>) valueProxy.getValue();
    }
}
