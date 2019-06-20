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

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;
import com.google.appengine.tools.pipeline.impl.util.ValueLocation;
import com.google.appengine.tools.pipeline.impl.util.ValueProxy;
import com.google.appengine.tools.pipeline.impl.util.ValueStoragePath;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A slot to be filled in with a value.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class Slot extends PipelineModelObject {
    public static final String DATA_STORE_KIND = "Slot";
    public static final String FILLED_PROPERTY = "filled";
    public static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
    public static final String FILL_TIME_PROPERTY = "fillTime";
    public static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";
    public static final String VALUE_LOCATION_PROPERTY = "valueLocation";
    public static final String DATABASE_VALUE_PROPERTY = "databaseValue";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    FILLED_PROPERTY,
                    WAITING_ON_ME_PROPERTY,
                    FILL_TIME_PROPERTY,
                    SOURCE_JOB_KEY_PROPERTY,
                    VALUE_LOCATION_PROPERTY,
                    DATABASE_VALUE_PROPERTY
            )
            .build();
    private final List<RecordKey> waitingOnMeKeys;
    // persistent
    private boolean filled;
    private Date fillTime;
    private UUID sourceJobKey;
    // transient
    private List<Barrier> waitingOnMeInflated;
    private ValueProxy valueProxy;

    public Slot(
            final PipelineManager pipelineManager,
            final UUID rootJobKey,
            final UUID generatorJobKey,
            final UUID graphKey
    ) {
        super(DATA_STORE_KIND, rootJobKey, generatorJobKey, graphKey);
        waitingOnMeKeys = new LinkedList<>();
        valueProxy = new ValueProxy(
                pipelineManager,
                null,
                new ValueStoragePath(getRootJobKey(), DATA_STORE_KIND, getKey())
        );
    }

    public Slot(final PipelineManager pipelineManager, @Nullable final String prefix, final StructReader entity) {
        this(pipelineManager, prefix, entity, false);
    }

    public Slot(final PipelineManager pipelineManager, @Nullable final String prefix, final StructReader entity, final boolean lazy) {
        super(DATA_STORE_KIND, prefix, entity);
        filled = !entity.isNull(Record.property(prefix, FILL_TIME_PROPERTY))
                && entity.getBoolean(Record.property(prefix, FILLED_PROPERTY));
        fillTime = entity.isNull(Record.property(prefix, FILL_TIME_PROPERTY)) ? null : entity.getTimestamp(Record.property(prefix, FILL_TIME_PROPERTY)).toDate();
        sourceJobKey = entity.isNull(Record.property(prefix, SOURCE_JOB_KEY_PROPERTY)) ? null : UUID.fromString(entity.getString(Record.property(prefix, SOURCE_JOB_KEY_PROPERTY)));
        waitingOnMeKeys = getRecordKeyListProperty(Record.property(prefix, WAITING_ON_ME_PROPERTY), entity).orElse(null);
        final String valueLocationProperty = Record.property(prefix, VALUE_LOCATION_PROPERTY);
        final String databaseValueProperty = Record.property(prefix, DATABASE_VALUE_PROPERTY);
        final String rootJobKeyProperty = Record.property(prefix, ROOT_JOB_KEY_PROPERTY);
        final String idProperty = Record.property(prefix, ID_PROPERTY);
        valueProxy = new ValueProxy(
                pipelineManager,
                entity.isNull(valueLocationProperty) ? ValueLocation.DATABASE : ValueLocation.valueOf(entity.getString(valueLocationProperty)),
                entity.isNull(databaseValueProperty) ? null : entity.getBytes(databaseValueProperty).toByteArray(),
                lazy,
                new ValueStoragePath(UUID.fromString(entity.getString(rootJobKeyProperty)), DATA_STORE_KIND, UUID.fromString(entity.getString(idProperty)))
        );
    }

    public static List<String> propertiesForSelect(@Nullable final String prefix) {
        return Record.propertiesForSelect(DATA_STORE_KIND, PROPERTIES, prefix);
    }

    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        //
        valueProxy.updateStorage(
                location -> entity.set(VALUE_LOCATION_PROPERTY).to(location.name()),
                databaseBlob -> entity.set(DATABASE_VALUE_PROPERTY).to(databaseBlob),
                (storageLocation, storageBlob) -> mutation.setValueMutation(new PipelineMutation.ValueMutation(
                        storageLocation,
                        storageBlob
                ))
        );
        entity.set(FILLED_PROPERTY).to(filled);
        if (null != fillTime) {
            entity.set(FILL_TIME_PROPERTY).to(Timestamp.of(fillTime));
        }
        if (null != sourceJobKey) {
            entity.set(SOURCE_JOB_KEY_PROPERTY).to(sourceJobKey.toString());
        }
        entity.set(WAITING_ON_ME_PROPERTY).toStringArray(
                waitingOnMeKeys == null
                        ? null
                        : waitingOnMeKeys.stream().map(RecordKey::sertialize).collect(Collectors.toList())
        );
        return mutation;
    }

    @Override
    public String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    public void inflate(final Map<RecordKey, Barrier> pool) {
        waitingOnMeInflated = buildInflated(waitingOnMeKeys, pool);
    }

    public void addWaiter(final Barrier waiter) {
        waitingOnMeKeys.add(new RecordKey(waiter.getRootJobKey(), waiter.getKey()));
        if (null == waitingOnMeInflated) {
            waitingOnMeInflated = new LinkedList<>();
        }
        waitingOnMeInflated.add(waiter);
    }

    public boolean isFilled() {
        return filled;
    }

    public Object getValue() {
        return valueProxy.getValue();
    }

    /**
     * Will return {@code null} if this slot is not filled.
     */
    public Date getFillTime() {
        return fillTime;
    }

    public UUID getSourceJobKey() {
        return sourceJobKey;
    }

    public void setSourceJobKey(final UUID key) {
        sourceJobKey = key;
    }

    public void fill(final Object value) {
        filled = true;
        valueProxy.setValue(value);
        fillTime = new Date();
    }

    public List<RecordKey> getWaitingOnMeKeys() {
        return waitingOnMeKeys;
    }

    /**
     * If this slot has not yet been inflated this method returns null;
     */
    public List<Barrier> getWaitingOnMeInflated() {
        return waitingOnMeInflated;
    }

    @Override
    public String toString() {
        return "Slot[" + getKeyName(getKey()) + ", valueProxy=" + valueProxy
                + ", filled=" + filled + ", waitingOnMe=" + waitingOnMeKeys + ", parent="
                + getKeyName(getGeneratorJobKey()) + ", graphKey=" + getGraphKey() + "]";
    }
}
