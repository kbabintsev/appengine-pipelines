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

import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

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
public abstract class PipelineModelObject {

    public static final String ROOT_JOB_KEY_PROPERTY = "rootJobKey";
    public static final String ID_PROPERTY = "id";
    private static final String GENERATOR_JOB_PROPERTY = "generatorJobKey";
    private static final String GRAPH_KEY_PROPERTY = "graphKey";
    protected static final List<String> BASE_PROPERTIES = ImmutableList.of(
            ROOT_JOB_KEY_PROPERTY,
            ID_PROPERTY,
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
    private final UUID rootJobKey;

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
     * @param tableName
     * @param rootJobKey      The key of the root job for this pipeline. This must be
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
            final String tableName, final UUID rootJobKey, final UUID thisKey, final UUID generatorJobKey, final UUID graphKey) {
        if (null == tableName) {
            throw new IllegalArgumentException("tableName is null");
        }
        if (null == rootJobKey) {
            throw new IllegalArgumentException("rootJobKey is null");
        }
        if (generatorJobKey == null && graphKey != null
                || generatorJobKey != null && graphKey == null) {
            throw new IllegalArgumentException(
                    "Either neither or both of generatorParentJobKey and graphKey must be set.");
        }
        this.tableName = tableName;
        this.rootJobKey = rootJobKey;
        this.generatorJobKey = generatorJobKey;
        this.graphKey = graphKey;
        if (null == thisKey) {
            key = UuidGenerator.nextUuid();
        } else {
            key = thisKey;
        }
    }

    /**
     * Construct a new PipelineModelObject with the given rootJobKey,
     * generatorJobKey, and graphKey, a newly generated key, and no entity group
     * parent.
     *
     * @param tableName
     * @param rootJobKey      The key of the root job for this pipeline. This must be
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
            final String tableName, final UUID rootJobKey, final UUID generatorJobKey, final UUID graphKey
    ) {
        this(tableName, rootJobKey, null, generatorJobKey, graphKey);
    }

    /**
     * Construct a new PipelineModelObject from the previously saved Entity.
     *
     * @param tableName
     * @param entity    An Entity obtained previously from a call to
     *                  {@link #toEntity()}.
     */
    protected PipelineModelObject(final String tableName, final StructReader entity) {
        this(tableName, extractRootJobKey(entity), extractKey(entity), extractGeneratorJobKey(entity),
                extractGraphKey(entity));
        final String expectedEntityType = getDatastoreKind();
        if (!expectedEntityType.equals(tableName)) {
            throw new IllegalArgumentException("The entity is not of kind " + expectedEntityType);
        }
    }

    private static UUID extractRootJobKey(final StructReader entity) {
        return entity.isNull(ROOT_JOB_KEY_PROPERTY) ? null : UUID.fromString(entity.getString(ROOT_JOB_KEY_PROPERTY));
    }

    private static UUID extractGeneratorJobKey(final StructReader entity) {
        return entity.isNull(GENERATOR_JOB_PROPERTY) ? null : UUID.fromString(entity.getString(GENERATOR_JOB_PROPERTY));
    }

    private static UUID extractGraphKey(final StructReader entity) {
        return entity.isNull(GRAPH_KEY_PROPERTY) ? null : UUID.fromString(entity.getString(GRAPH_KEY_PROPERTY));
    }

    private static UUID extractKey(final StructReader entity) {
        return UUID.fromString(entity.getString(ID_PROPERTY));
    }

    protected static <E> List<E> buildInflated(final Collection<UUID> listOfIds, final Map<UUID, E> pool) {
        final ArrayList<E> list = new ArrayList<>(listOfIds.size());
        for (final UUID id : listOfIds) {
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

    protected static String getKeyName(final UUID key) {
        return key == null ? "null" : key.toString();
    }

    public abstract PipelineMutation toEntity();

    protected final PipelineMutation toProtoEntity() {
        final Mutation.WriteBuilder writeBuilder = Mutation.newInsertOrUpdateBuilder(tableName);
        writeBuilder.set(ID_PROPERTY).to(key.toString());
        writeBuilder.set(ROOT_JOB_KEY_PROPERTY).to(rootJobKey.toString());
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

    public final UUID getRootJobKey() {
        return rootJobKey;
    }

    public final UUID getGeneratorJobKey() {
        return generatorJobKey;
    }

    public final UUID getGraphKey() {
        return graphKey;
    }

    protected abstract String getDatastoreKind();
}
