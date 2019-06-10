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

import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

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

    private final Throwable exception;

    public ExceptionRecord(
            final UUID rootJobKey, final UUID generatorJobKey, final UUID graphKey, final Throwable exception) {
        super(DATA_STORE_KIND, rootJobKey, generatorJobKey, graphKey);
        this.exception = exception;
    }

    public ExceptionRecord(final StructReader entity) {
        super(DATA_STORE_KIND, entity);
        final ByteArray serializedExceptionBlob = entity.getBytes(EXCEPTION_PROPERTY);
        final byte[] serializedException = serializedExceptionBlob.toByteArray();
        try {
            exception = (Throwable) SerializationUtils.deserialize(serializedException);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize exception for " + getKey(), e);
        }
    }

    public Throwable getException() {
        return exception;
    }

    @Override
    protected String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    @Override
    public PipelineMutation toEntity() {
        try {
            final PipelineMutation mutation = toProtoEntity();
            final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
            final byte[] serializedException = SerializationUtils.serialize(exception);
            entity.set(EXCEPTION_PROPERTY).to(ByteArray.copyFrom(serializedException));
            return mutation;
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize exception for " + getKey(), e);
        }
    }
}
