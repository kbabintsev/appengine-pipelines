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
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The parent class of all Pipeline model objects.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class PipelineModelObject implements Record {

    public static final String PIPELINE_KEY_PROPERTY = "pipelineKey";
    public static final String KEY_PROPERTY = "key";
    private static final String GENERATOR_JOB_PROPERTY = "generatorJobKey";
    private static final String GRAPH_KEY_PROPERTY = "graphKey";
    protected static final List<String> BASE_PROPERTIES = ImmutableList.of(
            PIPELINE_KEY_PROPERTY,
            KEY_PROPERTY,
            GENERATOR_JOB_PROPERTY,
            GRAPH_KEY_PROPERTY
    );

    private final String tableName;
    /**
     * Datastore key of this object
     */
    private final UUID key;

    /**
     * Datastore key of the root job identifying the Pipeline to which this object
     * belongs.
     */
    private final UUID pipelineKey;

    /**
     * Datastore key of the generator job of this object. Your generator job is
     * the job whose run() method created you and the rest of your local job
     * graph. The generator of the objects in the root job graph is null.
     */
    private final UUID generatorJobKey;

    /**
     * A UUID generated during the execution of a run() method of a generator job.
     * Each of the objects in a local job graph are marked with this UUID and the
     * generator job records the graphKey of each of it's child job graph. This
     * enables us to distinguish between a valid child job graph and one that is
     * orphaned. A child job graph is valid if its graphKey is equal to the
     * childGraphKey of its generator job.
     */
    private final UUID graphKey;

    /**
     * Construct a new PipelineModelObject from the provided data.
     *
     * @param tableName       The name of the table where entity is stored
     * @param pipelineKey     The key of the root job for this pipeline. This must be
     *                        non-null, except in the case that we are currently constructing the
     *                        root job. In that case {@code thisKey} and {@code egParentKey} must
     *                        both be null and this must be a {@link JobRecord}.
     * @param thisKey         The key for the object being constructed. If this is null
     *                        then a new key will be constructed.
     * @param generatorJobKey The key of the job whose run() method created this
     *                        object. This must be non-null unless this object is part of the root
     *                        job graph---i.e. the root job, or one of its barriers or slots.
     * @param graphKey        The unique key of the local graph of this object. This is
     *                        used to determine whether or not this object is orphaned. The object
     *                        is defined to be non-orphaned if its graphKey is equal to the
     *                        childGraphKey of its parent job. This must be non-null unless this
     *                        object is part of the root job graph---i.e. the root job, or one of
     *                        its barriers or slots.
     */
    protected PipelineModelObject(
            final String tableName, final UUID pipelineKey, final UUID thisKey, final UUID generatorJobKey, final UUID graphKey) {
        if (null == tableName) {
            throw new IllegalArgumentException("tableName is null");
        }
        if (null == pipelineKey) {
            throw new IllegalArgumentException("pipelineKey is null");
        }
        if (generatorJobKey == null && graphKey != null
                || generatorJobKey != null && graphKey == null) {
            throw new IllegalArgumentException(
                    "Either neither or both of generatorParentJobKey and graphKey must be set.");
        }
        this.tableName = tableName;
        this.pipelineKey = pipelineKey;
        this.generatorJobKey = generatorJobKey;
        this.graphKey = graphKey;
        if (null == thisKey) {
            key = UuidGenerator.nextUuid();
        } else {
            key = thisKey;
        }
    }

    /**
     * Construct a new PipelineModelObject with the given pipelineKey,
     * generatorJobKey, and graphKey, a newly generated key, and no entity group
     * parent.
     *
     * @param tableName       The name of the table where entity is stored
     * @param pipelineKey     The key of the root job for this pipeline. This must be
     *                        non-null, except in the case that we are currently constructing the
     *                        root job. In that case this must be a {@link JobRecord}.
     * @param generatorJobKey The key of the job whose run() method created this
     *                        object. This must be non-null unless this object is part of the root
     *                        job graph---i.e. the root job, or one of its barriers or slots.
     * @param graphKey        The unique key of the local graph of this object. This is
     *                        used to determine whether or not this object is orphaned. The object
     *                        is defined to be non-orphaned if its graphKey is equal to the
     *                        childGraphKey of its parent job. This must be non-null unless this
     *                        object is part of the root job graph---i.e. the root job, or one of
     *                        its barriers or slots.
     */
    protected PipelineModelObject(
            final String tableName, final UUID pipelineKey, final UUID generatorJobKey, final UUID graphKey
    ) {
        this(tableName, pipelineKey, null, generatorJobKey, graphKey);
    }

    /**
     * Construct a new PipelineModelObject from the previously saved Entity.
     *
     * @param tableName The name of the table where entity is stored
     * @param entity    An Entity obtained previously from a call to
     *                  {@link #toEntity()}.
     */
    protected PipelineModelObject(final String tableName, @Nullable final String prefix, final StructReader entity) {
        this(
                tableName,
                entity.isNull(Record.property(prefix, PIPELINE_KEY_PROPERTY)) ? null : UUID.fromString(entity.getString(Record.property(prefix, PIPELINE_KEY_PROPERTY))),
                UUID.fromString(entity.getString(Record.property(prefix, KEY_PROPERTY))),
                entity.isNull(Record.property(prefix, GENERATOR_JOB_PROPERTY)) ? null : UUID.fromString(entity.getString(Record.property(prefix, GENERATOR_JOB_PROPERTY))),
                entity.isNull(Record.property(prefix, GRAPH_KEY_PROPERTY)) ? null : UUID.fromString(entity.getString(Record.property(prefix, GRAPH_KEY_PROPERTY)))
        );
        final String expectedEntityType = getDatastoreKind();
        if (!expectedEntityType.equals(tableName)) {
            throw new IllegalArgumentException("The entity is not of kind " + expectedEntityType);
        }
    }

    protected static <E> List<E> buildInflated(final Collection<RecordKey> listOfIds, final Map<RecordKey, E> pool) {
        final ArrayList<E> list = new ArrayList<>(listOfIds.size());
        for (final RecordKey id : listOfIds) {
            final E x = pool.get(id);
            if (null == x) {
                throw new RuntimeException("No object found in pool with id=" + id);
            }
            list.add(x);
        }
        return list;
    }

    protected static Optional<List<Long>> getLongListProperty(final String propertyName, final StructReader entity) {
        if (entity.isNull(propertyName)) {
            return Optional.empty();
        }
        return Optional.of(Lists.newArrayList(entity.getLongList(propertyName)));
    }

    protected static Optional<List<UUID>> getUuidListProperty(final String propertyName, final StructReader entity) {
        if (entity.isNull(propertyName)) {
            return Optional.empty();
        }
        return Optional.of(entity.getStringList(propertyName).stream().map(UUID::fromString).collect(Collectors.toList()));
    }

    protected static Optional<List<RecordKey>> getRecordKeyListProperty(final String propertyName, final StructReader entity) {
        if (entity.isNull(propertyName)) {
            return Optional.empty();
        }
        return Optional.of(entity.getStringList(propertyName).stream().map(RecordKey::new).collect(Collectors.toList()));
    }

    protected static String getKeyName(final UUID key) {
        return key == null ? "null" : key.toString();
    }

    public static boolean isNullInJoin(@Nonnull final String prefix, @Nonnull final StructReader struct) {
        return struct.isNull(Record.property(prefix, KEY_PROPERTY));
    }

    @Override
    public abstract PipelineMutation toEntity();

    protected final PipelineMutation toProtoEntity() {
        final Mutation.WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(tableName);
        writeBuilder.set(KEY_PROPERTY).to(key.toString());
        writeBuilder.set(PIPELINE_KEY_PROPERTY).to(pipelineKey.toString());
        if (generatorJobKey != null) {
            writeBuilder.set(GENERATOR_JOB_PROPERTY).to(generatorJobKey.toString());
        }
        if (graphKey != null) {
            writeBuilder.set(GRAPH_KEY_PROPERTY).to(graphKey.toString());
        }
        return new PipelineMutation(writeBuilder);
    }

    public final UUID getKey() {
        return key;
    }

    public final UUID getPipelineKey() {
        return pipelineKey;
    }

    public final UUID getGeneratorJobKey() {
        return generatorJobKey;
    }

    public final UUID getGraphKey() {
        return graphKey;
    }

    @Override
    public abstract String getDatastoreKind();
}
