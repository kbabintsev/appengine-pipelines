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
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
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
public class Slot extends PipelineModelObject {

  public static final String DATA_STORE_KIND = "Slot";
  private static final String FILLED_PROPERTY = "filled";
  private static final String WAITING_ON_ME_PROPERTY = "waitingOnMe";
  private static final String FILL_TIME_PROPERTY = "fillTime";
  private static final String SOURCE_JOB_KEY_PROPERTY = "sourceJob";
  public static final List<String> PROPERTIES = ImmutableList.<String>builder()
          .addAll(BASE_PROPERTIES)
          .add(
                  FILLED_PROPERTY,
                  WAITING_ON_ME_PROPERTY,
                  FILL_TIME_PROPERTY,
                  SOURCE_JOB_KEY_PROPERTY
          )
          .build();

  // persistent
  private boolean filled;
  private Date fillTime;
  private UUID sourceJobKey;
  private final List<UUID> waitingOnMeKeys;

  // transient
  private List<Barrier> waitingOnMeInflated;
  private Object value;
  private boolean valueLoaded;


  public Slot(UUID rootJobKey, UUID generatorJobKey, String graphGUID) {
    super(DATA_STORE_KIND, rootJobKey, generatorJobKey, graphGUID);
    waitingOnMeKeys = new LinkedList<>();
  }

  public Slot(StructReader entity) {
    this(entity, false);
  }

  public Slot(StructReader entity, boolean lazy) {
    super(DATA_STORE_KIND, entity);
    filled = !entity.isNull(FILL_TIME_PROPERTY)
            && entity.getBoolean(FILLED_PROPERTY);
    fillTime = entity.isNull(FILL_TIME_PROPERTY) ? null : entity.getTimestamp(FILL_TIME_PROPERTY).toDate();
    sourceJobKey = entity.isNull(SOURCE_JOB_KEY_PROPERTY) ? null : UUID.fromString(entity.getString(SOURCE_JOB_KEY_PROPERTY));
    waitingOnMeKeys = getUuidListProperty(WAITING_ON_ME_PROPERTY, entity).orElse(null);
    if (lazy) {
      value = null;
      valueLoaded = false;
    } else {
      try {
        value = SerializationUtils.deserialize(PipelineManager.getBackEnd().retrieveBlob(getRootJobKey(), getKey()));
      } catch (IOException e) {
        throw new RuntimeException("Can't deserialize value", e);
      }
      valueLoaded = true;
    }
  }

  @Override
  public PipelineMutation toEntity() {
    PipelineMutation mutation = toProtoEntity();
    final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
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
                    : waitingOnMeKeys.stream().map(UUID::toString).collect(Collectors.toList())
    );
    if (value != null) {
      try {
        mutation.setBlobMutation(new PipelineMutation.BlobMutation(
                getRootJobKey(),
                getKey(),
                SerializationUtils.serialize(value)
        ));
      } catch (IOException e) {
        throw new RuntimeException("Can't serialize value", e);
      }
    }
    return mutation;
  }

  @Override
  protected String getDatastoreKind() {
    return DATA_STORE_KIND;
  }

  public void inflate(Map<UUID, Barrier> pool) {
    waitingOnMeInflated = buildInflated(waitingOnMeKeys, pool);
  }

  public void addWaiter(Barrier waiter) {
    waitingOnMeKeys.add(waiter.getKey());
    if (null == waitingOnMeInflated) {
      waitingOnMeInflated = new LinkedList<>();
    }
    waitingOnMeInflated.add(waiter);
  }

  public boolean isFilled() {
    return filled;
  }

  public Object getValue() {
    if (!valueLoaded) {
      try {
        value = SerializationUtils.deserialize(PipelineManager.getBackEnd().retrieveBlob(getRootJobKey(), getKey()));
      } catch (IOException e) {
        throw new RuntimeException("Can't deserialize value", e);
      }
      valueLoaded = true;
    }
    return value;
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

  public void setSourceJobKey(UUID key) {
    sourceJobKey = key;
  }

  public void fill(Object value) {
    filled = true;
    this.value = value;
    this.valueLoaded = true;
    fillTime = new Date();
  }

  public List<UUID> getWaitingOnMeKeys() {
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
    return "Slot[" + getKeyName(getKey()) + ", value=" + (valueLoaded ? value : "...")
        + ", filled=" + filled + ", waitingOnMe=" + waitingOnMeKeys + ", parent="
        + getKeyName(getGeneratorJobKey()) + ", guid=" + getGraphGuid() + "]";
  }
}
