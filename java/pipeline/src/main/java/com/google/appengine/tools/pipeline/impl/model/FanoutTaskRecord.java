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

import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * A datastore entity for storing data necessary for a fan-out task
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class FanoutTaskRecord extends PipelineModelObject {

    public static final String DATA_STORE_KIND = "FanoutTask";
    private static final String PAYLOAD_PROPERTY = "payload";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    PAYLOAD_PROPERTY
            )
            .build();

    private final byte[] payload;

    public FanoutTaskRecord(final UUID pipelineKey, final byte[] payload) {
        super(DATA_STORE_KIND, pipelineKey, null, null);
        if (payload == null) {
            throw new RuntimeException("Payload must not be null");
        }
        this.payload = payload;
    }

    public FanoutTaskRecord(@Nullable final String prefix, final StructReader entity) {
        super(DATA_STORE_KIND, prefix, entity);
        final ByteArray payloadBlob = entity.getBytes(Record.property(prefix, PAYLOAD_PROPERTY));
        payload = payloadBlob.toByteArray();
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        entity.set(PAYLOAD_PROPERTY).to(ByteArray.copyFrom(payload));
        return mutation;
    }
}
