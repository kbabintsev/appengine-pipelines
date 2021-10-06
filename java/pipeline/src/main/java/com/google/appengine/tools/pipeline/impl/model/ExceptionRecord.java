// Copyright 2013 Google Inc.
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

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * A datastore entity for storing information about job failure.
 *
 * @author maximf@google.com (Maxim Fateev)
 */
public final class ExceptionRecord extends PipelineModelObject {

    public static final String DATA_STORE_KIND = "Exception";
    private static final String EXCEPTION_PROPERTY = "exceptionBytes";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    EXCEPTION_PROPERTY
            )
            .build();

    private final PipelineManager pipelineManager;
    private final Throwable exception;

    public ExceptionRecord(
            final PipelineManager pipelineManager,
            final UUID pipelineKey,
            final UUID generatorJobKey,
            final UUID graphKey,
            final Throwable exception
    ) {
        super(DATA_STORE_KIND, pipelineKey, generatorJobKey, graphKey);
        this.pipelineManager = pipelineManager;
        this.exception = exception;
    }

    public ExceptionRecord(final PipelineManager pipelineManager, @Nullable final String prefix, final StructReader entity) {
        super(DATA_STORE_KIND, prefix, entity);
        this.pipelineManager = pipelineManager;
        final ByteArray serializedExceptionBlob = entity.getBytes(Record.property(prefix, EXCEPTION_PROPERTY));
        final byte[] serializedException = serializedExceptionBlob.toByteArray();
        try {
            exception = (Throwable) SerializationUtils.deserialize(serializedException);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize exception for " + getKey(), e);
        }
    }

    public static List<String> propertiesForSelect(@Nullable final String prefix) {
        return Record.propertiesForSelect(DATA_STORE_KIND, PROPERTIES, prefix);
    }

    public Throwable getException() {
        return exception;
    }

    @Override
    public String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        byte[] serializedException;
        try {
            serializedException = SerializationUtils.serialize(exception);
        } catch (IOException e) {
            try {
                serializedException = SerializationUtils.serialize(new SerializableException(exception));
            } catch (IOException innerEx) {
                throw new RuntimeException("Failed to serialize original exception and it's serializable wrapper for " + getKey(), innerEx);
            }
        }
        entity.set(EXCEPTION_PROPERTY).to(ByteArray.copyFrom(serializedException));
        return mutation;
    }
}
