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

import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.PipelineRecord;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.ValueStoragePath;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * An interface that gives access to data store and task queue operations that
 * must be performed during the execution of a Pipeline.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface PipelineBackEnd {

    /**
     * Saves entities to the data store and enqueues tasks to the task queue based
     * on the specification given in {@code UpdateSpec}. See the remarks at the
     * top of {@link UpdateSpec} for details.
     */
    void save(UpdateSpec updateSpec, QueueSettings queueSettings);

    /**
     * Saves an {@code UpdateSpec} to the data store, but transactionally checks
     * that a certain condition is true before committing the final transaction.
     * <p>
     * See the remarks at the top of {@link UpdateSpec} for more information about
     * {@code UpdateSpecs}. As part of the
     * {@link UpdateSpec#getFinalTransaction() final transaction} the
     * {@link JobRecord} with the given {@code jobKey} will be retrieved from the
     * data store and its {@link JobRecord#getState() state} will be checked to
     * see if it is one of the {@code expectedStates}. If not then the final
     * transaction will be aborted, and this method will return {@code false}.
     *
     * @return {@code true} iff the transaction was applied successfully.
     */
    boolean saveWithJobStateCheck(UpdateSpec updateSpec, QueueSettings queueSettings,
                                  UUID jobKey, JobRecord.State... expectedStates);

    /**
     * Get the JobRecord with the given Key from the data store, and optionally
     * also get some of the Barriers and Slots associated with it.
     *
     * @param key           The key of the JobRecord to be fetched
     * @param inflationType Specifies the manner in which the returned JobRecord
     *                      should be inflated.
     * @return A {@code JobRecord}, possibly with a partially-inflated associated
     * graph of objects.
     * @throws NoSuchObjectException If Either the JobRecord or any of the
     *                               associated Slots or Barriers are not found in the data store.
     */
    JobRecord queryJob(UUID rootJobKey, UUID key, JobRecord.InflationType inflationType) throws NoSuchObjectException;

    /**
     * Get the JobRecord with the given Key from the data store, and optionally
     * also get some of the Barriers and Slots associated with it.
     *
     * @param pipelineKey   The key of the JobRecord to be fetched
     * @param inflationType Specifies the manner in which the returned JobRecord
     *                      should be inflated.
     * @return A {@code JobRecord}, possibly with a partially-inflated associated
     * graph of objects.
     * @throws NoSuchObjectException If Either the JobRecord or any of the
     *                               associated Slots or Barriers are not found in the data store.
     */
    PipelineRecord queryPipeline(UUID pipelineKey) throws NoSuchObjectException;

    /**
     * Get the Slot with the given Key from the data store, and optionally also
     * get the Barriers that are waiting on the Slot, and the other Slots that
     * those Barriers are waiting on.
     *
     * @param key     The Key of the slot to fetch.
     * @param inflate If this is {@code true} then the Barriers that are waiting
     *                on the Slot and the other Slots that those Barriers are waiting on
     *                will also be fetched from the data store and used to partially
     *                populate the graph of objects attached to the returned Slot. In
     *                particular: {@link Slot#getWaitingOnMeInflated()} will not return
     *                {@code null} and also that for each of the
     *                {@link com.google.appengine.tools.pipeline.impl.model.Barrier Barriers}
     *                returned from that method
     *                {@link com.google.appengine.tools.pipeline.impl.model.Barrier#getWaitingOnInflated()}
     *                will not return {@code null}.
     * @return A {@code Slot}, possibly with a partially-inflated associated graph
     * of objects.
     * @throws NoSuchObjectException if unable to find an entity
     */
    Slot querySlot(UUID rootJobKey, UUID key, boolean inflate) throws NoSuchObjectException;

    /**
     * Get the Failure with the given Key from the data store.
     *
     * @param key The Key of the failure to fetch.
     * @return A {@code FailureRecord}
     * @throws NoSuchObjectException if unable to find an entity
     */
    ExceptionRecord queryFailure(UUID rootJobKey, UUID key) throws NoSuchObjectException;

    /**
     * Enqueues to the App Engine task queue the tasks encoded by the given
     * {@code FanoutTask}. This method is invoked from within the task handler for
     * a FanoutTask. See the comments at the top of {@link FanoutTask} for more
     * details.
     *
     * @param fanoutTask The FanoutTask to handle
     * @throws NoSuchObjectException If the
     *                               {@link com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord}
     *                               specified by the {@link FanoutTask#getRecordKey() key} contained in
     *                               {@code fanoutTask} does not exist in the data store.
     */
    void handleFanoutTask(FanoutTask fanoutTask) throws NoSuchObjectException;

    Set<UUID> getTestPipelines();

    /**
     * Queries the data store for all Pipeline objects associated with the given
     * root Job Key
     */
    PipelineObjects queryFullPipeline(UUID rootJobKey);

    /**
     * Delete all datastore entities corresponding to the given pipeline.
     *
     * @param rootJobKey The root job key identifying the pipeline
     * @param force      If this parameter is not {@code true} then this method will
     *                   throw an {@link IllegalStateException} if the specified pipeline is
     *                   not in the
     *                   {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED}
     *                   or
     *                   {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED}
     *                   state.
     * @param async      If this parameter is {@code true} then instead of performing
     *                   the delete operation synchronously, this method will enqueue a task
     *                   to perform the operation.
     * @throws IllegalStateException If {@code force = false} and the specified
     *                               pipeline is not in the
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED}
     *                               or
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED}
     *                               state.
     */
    void deletePipeline(UUID rootJobKey, boolean force, boolean async)
            throws IllegalStateException;

    void cleanBlobs(String prefix);

    void saveBlob(ValueStoragePath valueStoragePath, byte[] value);

    byte[] retrieveBlob(ValueStoragePath valueStoragePath);

    /**
     * Immediately enqueues the given task in the app engine task queue. Note that
     * there is another way to enqueue a task, namely to register the task with
     * {@link UpdateSpec.TransactionWithTasks#registerTask} that is associated
     * with the {@link UpdateSpec#getFinalTransaction() final transaction} of an
     * {@link UpdateSpec}. This method is simpler if one only wants to enqueue a
     * single task in isolation.
     */
    void enqueue(Task task);

    /**
     * Queries the data store for all root Pipeline.
     *
     * @param classFilter An optional filter by class display name.
     * @param inStates    An optional filter by states of job.
     * @param limit       Results limit (zero or negative will be treated as no limit).
     * @param offset      Results offset (zero or negative will be treated as no offset).
     * @return a Pair of job records and a next cursor (or null, if no more results).
     */
    List<PipelineRecord> queryRootPipelines(
            @Nullable String classFilter,
            @Nullable Set<JobRecord.State> inStates,
            int limit,
            int offset
    );

    /**
     * Returns the set of all root pipelines display name.
     */
    Set<String> getRootPipelinesDisplayName();

    void shutdown();
}

