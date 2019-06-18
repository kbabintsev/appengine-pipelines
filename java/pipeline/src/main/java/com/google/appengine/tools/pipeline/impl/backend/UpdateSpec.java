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

package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A specification of multiple updates to the state of the Pipeline model
 * objects that should be persisted to the data store, as well as new tasks that
 * should be enqueued.
 * <p>
 * An {@code UpdateSpec} is organized into the following sub-groups:
 * <ol>
 * <li>A {@link #newTransaction(String)} named transactions}
 * <li>A {@link #getFinalTransaction() final transactional group}.
 * </ol>
 * <p>
 * When an {@code UpdateSpec} is {@link PipelineBackEnd#save saved},
 * the groups will be saved in the following order using the following
 * transactions:
 * <ol>
 * <li>Each element of the {@link #newTransaction(String)} named transactions}
 * will be saved transactionally in the order they been added. Then,
 * <li>each element of the {@link #getFinalTransaction() final transactional
 * group} will be saved in a transaction.
 * </ol>
 * <p>
 * The {@link #getFinalTransaction() final transactional group} is a
 * {@link TransactionWithTasks} and so may contain {@link Task tasks}. The tasks
 * will be enqueued in the final transaction. If there are too many tasks to
 * enqueue in a single transaction then instead a single
 * {@link com.google.appengine.tools.pipeline.impl.tasks.FanoutTask} will be
 * enqueued.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class UpdateSpec {

    private static final int INITIAL_MAP_CAPACITY = 10;
    private List<Transaction> transactions = new ArrayList<>();
    private TransactionWithTasks finalTransaction = new TransactionWithTasks("final");
    private UUID rootJobKey;

    public UpdateSpec(final UUID rootJobKey) {
        this.rootJobKey = rootJobKey;
    }

    public UUID getRootJobKey() {
        return rootJobKey;
    }

    public void setRootJobKey(final UUID rootJobKey) {
        this.rootJobKey = rootJobKey;
    }

    public TransactionWithTasks getFinalTransaction() {
        return finalTransaction;
    }

    public Transaction newTransaction(final String name) {
        final Transaction out = new Transaction(name);
        transactions.add(out);
        return out;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    /**
     * A group of Pipeline model objects that should be saved to the data store.
     * The model object types are:
     * <ol>
     * <li> {@link Barrier}
     * <li> {@link Slot}
     * <li> {@link JobRecord}
     * <li> {@link JobInstanceRecord}
     * </ol>
     * The objects are stored in maps keyed by their {@link UUID}, so there is no
     * danger of inadvertently adding the same object twice.
     *
     * @author rudominer@google.com (Mitch Rudominer)
     */
    public static class Transaction {
        private static final int INITIAL_SIZE = 20;

        private final String name;
        private Map<UUID, JobRecord> jobMap = new HashMap<>(INITIAL_SIZE);
        private Map<UUID, Barrier> barrierMap = new HashMap<>(INITIAL_SIZE);
        private Map<UUID, Slot> slotMap = new HashMap<>(INITIAL_SIZE);
        private Map<UUID, JobInstanceRecord> jobInstanceMap = new HashMap<>(INITIAL_SIZE);
        private Map<UUID, ExceptionRecord> failureMap = new HashMap<>(INITIAL_SIZE);

        public Transaction(final String name) {
            this.name = name;
        }

        public final String getName() {
            return name;
        }

        private static <E extends PipelineModelObject> void put(final Map<UUID, E> map, final E object) {
            map.put(object.getKey(), object);
        }

        /**
         * Include the given Barrier in the group of objects to be saved.
         */
        public final void includeBarrier(final Barrier barrier) {
            put(barrierMap, barrier);
        }

        public final Collection<Barrier> getBarriers() {
            return barrierMap.values();
        }

        /**
         * Include the given JobRecord in the group of objects to be saved.
         */
        public final void includeJob(final JobRecord job) {
            put(jobMap, job);
        }

        public final Collection<JobRecord> getJobs() {
            return jobMap.values();
        }

        /**
         * Include the given Slot in the group of objects to be saved.
         */
        public final void includeSlot(final Slot slot) {
            put(slotMap, slot);
        }

        public final Collection<Slot> getSlots() {
            return slotMap.values();
        }

        /**
         * Include the given JobInstanceRecord in the group of objects to be saved.
         */
        public final void includeJobInstanceRecord(final JobInstanceRecord record) {
            put(jobInstanceMap, record);
        }

        public final Collection<JobInstanceRecord> getJobInstanceRecords() {
            return jobInstanceMap.values();
        }

        public final void includeException(final ExceptionRecord failureRecord) {
            put(failureMap, failureRecord);
        }

        public final Collection<ExceptionRecord> getFailureRecords() {
            return failureMap.values();
        }
    }

    /**
     * An extension of {@link Transaction} that also accepts
     * {@link Task Tasks}. Each task included in the group will
     * be enqueued to the task queue as part of the same transaction
     * in which the objects are saved. If there are too many tasks
     * to include in a single transaction then a
     * {@link com.google.appengine.tools.pipeline.impl.tasks.FanoutTask} will be
     * used.
     *
     * @author rudominer@google.com (Mitch Rudominer)
     */
    public class TransactionWithTasks extends Transaction {
        private static final int INITIAL_SIZE = 20;
        private final Set<Task> taskSet = new HashSet<>(INITIAL_SIZE);

        public TransactionWithTasks(final String name) {
            super(name);
        }

        public final void registerTask(final Task task) {
            taskSet.add(task);
        }

        /**
         * @return Unmodifiable collection of Tasks.
         */
        public final Collection<Task> getTasks() {
            return Collections.unmodifiableCollection(taskSet);
        }
    }
}
