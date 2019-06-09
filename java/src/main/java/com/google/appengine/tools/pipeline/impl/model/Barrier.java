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

import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@code Barrier} represents a list of slots that need to be filled before
 * something is allowed to happen.
 * <p>
 * There are two types of barriers, run barriers and finalize barriers. A run
 * barrier is used to trigger the running of a job. Its list of slots represent
 * arguments to the job. A finalize barrier is used to trigger the finalization
 * of a job. It has only one slot which is used as the output value of the job.
 * The essential properties are:
 * <ul>
 * <li>type: Either run or finalize
 * <li>jobKey: The datastore key of the associated job
 * <li>waitingOn: A list of the datastore keys of the slots for which this
 * barrier is waiting
 * <li>released: A boolean representing whether or not this barrier is released.
 * Released means that all of the slots are filled and so the action associated
 * with this barrier should be triggered.
 * </ul>
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class Barrier extends PipelineModelObject {

    public static final String DATA_STORE_KIND = "Barrier";
    private static final String TYPE_PROPERTY = "barrierType";
    private static final String JOB_KEY_PROPERTY = "jobKey";
    private static final String RELEASED_PROPERTY = "released";
    private static final String WAITING_ON_KEYS_PROPERTY = "waitingOnKeys";
    private static final String WAITING_ON_GROUP_SIZES_PROPERTY = "waitingOnGroupSizes";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    TYPE_PROPERTY,
                    JOB_KEY_PROPERTY,
                    RELEASED_PROPERTY,
                    WAITING_ON_KEYS_PROPERTY,
                    WAITING_ON_GROUP_SIZES_PROPERTY
            )
            .build();
    // persistent
    private final Type type;
    private final UUID jobKey;
    private final List<UUID> waitingOnKeys;
    private final List<Long> waitingOnGroupSizes;
    private boolean released;
    // transient
    private List<SlotDescriptor> waitingOnInflated;

    private Barrier(final Type type, final UUID rootJobKey, final UUID jobKey, final UUID generatorJobKey, final UUID graphKey) {
        super(DATA_STORE_KIND, rootJobKey, getEgParentKey(type, jobKey), null, generatorJobKey, graphKey);
        this.jobKey = jobKey;
        this.type = type;
        waitingOnGroupSizes = new LinkedList<>();
        waitingOnInflated = new LinkedList<>();
        waitingOnKeys = new LinkedList<>();
    }

    public Barrier(final Type type, final JobRecord jobRecord) {
        this(type, jobRecord.getRootJobKey(), jobRecord.getKey(), jobRecord.getGeneratorJobKey(),
                jobRecord.getGraphKey());
    }

    public Barrier(final StructReader entity) {
        super(DATA_STORE_KIND, entity);
        jobKey = UUID.fromString(entity.getString(JOB_KEY_PROPERTY)); // probably not null?
        type = Type.valueOf(entity.getString(TYPE_PROPERTY)); // probably not null?
        released = entity.getBoolean(RELEASED_PROPERTY); // probably not null?
        waitingOnKeys = getUuidListProperty(WAITING_ON_KEYS_PROPERTY, entity).orElse(null);
        waitingOnGroupSizes = getLongListProperty(WAITING_ON_GROUP_SIZES_PROPERTY, entity).orElse(null);
    }

    /**
     * Returns the entity group parent of a Barrier of the specified type.
     * <p>
     * According to our <a href="http://goto/java-pipeline-model">transactional
     * model</a>: If B is the finalize barrier of a Job J, then the entity group
     * parent of B is J. Run barriers do not have an entity group parent.
     */
    private static UUID getEgParentKey(final Type type, final UUID jobKey) {
        switch (type) {
            case RUN:
                return null;
            case FINALIZE:
                if (null == jobKey) {
                    throw new IllegalArgumentException("jobKey is null");
                }
                break;
            default:
                break;
        }
        return jobKey;
    }

    public static Barrier dummyInstanceForTesting() {
        final UUID dummyKey = UUID.fromString("00000000-0000-0000-0000-000000000bad");
        return new Barrier(Type.RUN, dummyKey, dummyKey, dummyKey, dummyKey);
    }

    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        entity.set(JOB_KEY_PROPERTY).to(jobKey.toString());
        entity.set(TYPE_PROPERTY).to(type.toString());
        entity.set(RELEASED_PROPERTY).to(released);
        entity.set(WAITING_ON_KEYS_PROPERTY).toStringArray(waitingOnKeys.stream().map(UUID::toString).collect(Collectors.toList()));
        entity.set(WAITING_ON_GROUP_SIZES_PROPERTY).toInt64Array(waitingOnGroupSizes);
        return mutation;
    }

    @Override
    protected String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    public void inflate(final Map<UUID, Slot> pool) {
        final int numSlots = waitingOnKeys.size();
        waitingOnInflated = new ArrayList<>(numSlots);
        for (int i = 0; i < numSlots; i++) {
            final UUID key = waitingOnKeys.get(i);
            final int groupSize = waitingOnGroupSizes.get(i).intValue();
            final Slot slot = pool.get(key);
            if (null == slot) {
                throw new RuntimeException("No slot in pool with key=" + key);
            }
            final SlotDescriptor descriptor = new SlotDescriptor(slot, groupSize);
            waitingOnInflated.add(descriptor);
        }
    }

    public UUID getJobKey() {
        return jobKey;
    }

    public Type getType() {
        return type;
    }

    public boolean isReleased() {
        return released;
    }

    public void setReleased() {
        released = true;
    }

    public List<UUID> getWaitingOnKeys() {
        return waitingOnKeys;
    }

    /**
     * May return null if this Barrier has not been inflated
     */
    public List<SlotDescriptor> getWaitingOnInflated() {
        return waitingOnInflated;
    }

    public Object[] buildArgumentArray() {
        final List<Object> argumentList = buildArgumentList();
        final Object[] argumentArray = new Object[argumentList.size()];
        argumentList.toArray(argumentArray);
        return argumentArray;
    }

    public List<Object> buildArgumentList() {
        if (null == waitingOnInflated) {
            throw new RuntimeException("" + this + " has not been inflated.");
        }
        final List<Object> argumentList = new LinkedList<>();
        ArrayList<Object> currentListArg = null;
        int currentListArgExpectedSize = -1;
        for (final SlotDescriptor descriptor : waitingOnInflated) {
            if (!descriptor.getSlot().isFilled()) {
                throw new RuntimeException("Slot is not filled: " + descriptor.getSlot());
            }
            final Object nextValue = descriptor.getSlot().getValue();
            if (currentListArg != null) {
                // Assert: currentListArg.size() < currentListArgExpectedSize
                if (descriptor.getGroupSize() != currentListArgExpectedSize + 1) {
                    throw new RuntimeException("expectedGroupSize: " + currentListArgExpectedSize
                            + ", groupSize: " + descriptor.getGroupSize() + "; nextValue=" + nextValue);
                }
                currentListArg.add(nextValue);
            } else {
                if (descriptor.getGroupSize() > 0) {
                    // We are not in the midst of a list and this element indicates
                    // a new list is starting. This element itself is a dummy
                    // marker, its value is ignored. The list is comprised
                    // of the next groupSize - 1 elements.
                    currentListArgExpectedSize = descriptor.getGroupSize() - 1;
                    currentListArg = new ArrayList<>(currentListArgExpectedSize);
                    argumentList.add(currentListArg);
                } else if (descriptor.getGroupSize() == 0) {
                    // We are not in the midst of a list and this element is not part of
                    // a list
                    argumentList.add(nextValue);
                } else {
                    // We were not in the midst of a list and this element is phantom
                }
            }
            if (null != currentListArg && currentListArg.size() == currentListArgExpectedSize) {
                // We have finished with the currentListArg
                currentListArg = null;
                currentListArgExpectedSize = -1;
            }
        }
        return argumentList;
    }

    private void addSlotDescriptor(final SlotDescriptor slotDescr) {
        if (null == waitingOnInflated) {
            waitingOnInflated = new LinkedList<>();
        }
        waitingOnInflated.add(slotDescr);
        waitingOnGroupSizes.add((long) slotDescr.getGroupSize());
        final Slot slot = slotDescr.getSlot();
        slot.addWaiter(this);
        waitingOnKeys.add(slotDescr.getSlot().getKey());
    }

    public void addRegularArgumentSlot(final Slot slot) {
        verifyStateBeforAdd(slot);
        addSlotDescriptor(new SlotDescriptor(slot, 0));
    }

    public void addPhantomArgumentSlot(final Slot slot) {
        verifyStateBeforAdd(slot);
        addSlotDescriptor(new SlotDescriptor(slot, -1));
    }

    private void verifyStateBeforAdd(final Slot slot) {
        if (getType() == Type.FINALIZE && waitingOnInflated != null && !waitingOnInflated.isEmpty()) {
            throw new IllegalStateException("Trying to add a slot, " + slot
                    + ", to an already populated finalized barrier: " + this);
        }
    }

    /**
     * Adds multiple slots to this barrier's waiting-on list representing a single
     * Job argument of type list.
     *
     * @param slotList A list of slots that will be added to the barrier and used
     *                 as the elements of the list Job argument.
     * @throws IllegalArgumentException if intialSlot is not filled.
     */
    public void addListArgumentSlots(final Slot initialSlot, final List<Slot> slotList) {
        if (!initialSlot.isFilled()) {
            throw new IllegalArgumentException("initialSlot must be filled");
        }
        verifyStateBeforAdd(initialSlot);
        final int groupSize = slotList.size() + 1;
        addSlotDescriptor(new SlotDescriptor(initialSlot, groupSize));
        for (final Slot slot : slotList) {
            addSlotDescriptor(new SlotDescriptor(slot, groupSize));
        }
    }

    @Override
    public String toString() {
        return "Barrier[" + getKeyName(getKey()) + ", " + type + ", released=" + released + ", "
                + jobKey + ", waitingOn="
                + StringUtils.toStringParallel(waitingOnKeys, waitingOnGroupSizes) + ", job="
                + getKeyName(getJobKey()) + ", parent="
                + getKeyName(getGeneratorJobKey()) + ", graphKey=" + getGraphKey() + "]";
    }

    /**
     * The type of Barrier
     */
    public enum Type {
        RUN, FINALIZE
    }
}
