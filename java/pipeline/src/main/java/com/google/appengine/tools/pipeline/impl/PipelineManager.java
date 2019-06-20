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

package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.FutureList;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.ImmediateValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd;
import com.google.appengine.tools.pipeline.impl.backend.PipelineTaskQueue;
import com.google.appengine.tools.pipeline.impl.backend.UpdateSpec;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.InflationType;
import com.google.appengine.tools.pipeline.impl.model.JobRecord.State;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.PipelineRecord;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.SlotDescriptor;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.tasks.CancelJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.DelayedSlotFillTask;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleChildExceptionTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import com.google.common.collect.Lists;
import com.google.inject.Injector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The central hub of the Pipeline implementation.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
public final class PipelineManager {

    private static final Logger LOGGER = Logger.getLogger(PipelineManager.class.getName());

    private final Injector injector;
    private final PipelineBackEnd backEnd;
    private final PipelineTaskQueue taskQueue;

    @Inject
    public PipelineManager(final Injector injector, final PipelineBackEnd backEnd, final PipelineTaskQueue taskQueue) {
        this.injector = injector;
        this.backEnd = backEnd;
        this.taskQueue = taskQueue;
    }

    /**
     * Creates and launches a new Pipeline
     * <p>
     * Creates the root Job with its associated Barriers and Slots and saves them
     * to the data store. All slots in the root job are immediately filled with
     * the values of the parameters to this method, and
     * {@link HandleSlotFilledTask HandleSlotFilledTasks} are enqueued for each of
     * the filled slots.
     *
     * @param settings    JobSetting array used to control details of the Pipeline
     * @param jobInstance A user-supplied instance of {@link Job} that will serve
     *                    as the root job of the Pipeline.
     * @param parameters  Arguments to the root job's run() method
     * @return The pipelineId of the newly created pipeline, also known as the rootJobId.
     */
    public UUID startNewPipeline(
            final JobSetting[] settings,
            final Job<?> jobInstance,
            final Object... parameters
    ) {
        final UUID pipelineKey = UuidGenerator.nextUuid();
        final PipelineRecord pipeline = new PipelineRecord(pipelineKey, jobInstance.getJobDisplayName());
        final UpdateSpec updateSpec = new UpdateSpec(pipelineKey);
        final UpdateSpec.Transaction transaction = updateSpec.newTransaction("startNewPipeline:" + pipelineKey);
        transaction.includePipeline(pipeline);
        Object[] params = parameters;
        Job<?> rootJobInstance = jobInstance;
        // If rootJobInstance has exceptionHandler it has to be wrapped to ensure that root job
        // ends up in finalized state in case of exception of run method and
        // exceptionHandler returning a result.
        if (JobRecord.hasExceptionHandler(jobInstance)) {
            rootJobInstance = new RootJobInstance(jobInstance, settings, params);
            params = new Object[0];
        }
        // Create the root Job and its associated Barriers and Slots
        // Passing null for parent JobRecord and graphKey
        // Create HandleSlotFilledTasks for the input parameters.
        final JobRecord jobRecord = registerNewRootJobRecord(updateSpec, transaction, pipelineKey, settings, rootJobInstance, params);
        // Save the Pipeline model objects and enqueue the tasks that start the Pipeline executing.
        backEnd.save(updateSpec, jobRecord.getQueueSettings());
        return jobRecord.getKey();
    }

    /**
     * Creates a new JobRecord with its associated Barriers and Slots. Also
     * creates new {@link HandleSlotFilledTask} for any inputs to the Job that are
     * immediately specified. Registers all newly created objects with the
     * provided {@code UpdateSpec} for later saving.
     * <p>
     * This method is called when starting a new Pipeline, in which case it is
     * used to create the root job, and it is called from within the run() method
     * of a generator job in order to create a child job.
     *
     * @param updateSpec  The {@code UpdateSpec} with which to register all newly
     *                    created objects. All objects will be added to the
     *                    {@link UpdateSpec#newTransaction(String) new transaction}
     *                    of the {@code UpdateSpec}.
     * @param transaction
     * @param settings    Array of {@code JobSetting} to apply to the newly created
     *                    JobRecord.
     * @param jobInstance The user-supplied instance of {@code Job} that
     *                    implements the Job that the newly created JobRecord represents.
     * @param params      The arguments to be passed to the run() method of the newly
     *                    created Job. Each argument may be an actual value or it may be an
     *                    object of type {@link Value} representing either an
     *                    {@link ImmediateValue} or a
     *                    {@link FutureValue FutureValue}.
     *                    For each element of the array, if the Object is not of type
     *                    {@link Value} then it is interpreted as an {@link ImmediateValue}
     *                    with the given Object as its value.
     * @return The newly constructed JobRecord.
     */
    public JobRecord registerNewRootJobRecord(
            @Nonnull final UpdateSpec updateSpec,
            @Nonnull final UpdateSpec.Transaction transaction,
            @Nonnull final UUID providedPipelineKey,
            @Nonnull final JobSetting[] settings,
            @Nonnull final Job<?> jobInstance,
            @Nonnull final Object[] params
    ) {
        final JobRecord jobRecord = new JobRecord(this, providedPipelineKey, jobInstance, settings);
        injectJobMembers(jobInstance);
        return registerNewJobRecord(updateSpec, transaction, jobRecord, params);
    }

    /**
     * Creates a new JobRecord with its associated Barriers and Slots. Also
     * creates new {@link HandleSlotFilledTask} for any inputs to the Job that are
     * immediately specified. Registers all newly created objects with the
     * provided {@code UpdateSpec} for later saving.
     * <p>
     * This method is called when starting a new Pipeline, in which case it is
     * used to create the root job, and it is called from within the run() method
     * of a generator job in order to create a child job.
     *
     * @param updateSpec   The {@code UpdateSpec} with which to register all newly
     *                     created objects. All objects will be added to the
     *                     {@link UpdateSpec#newTransaction(String) new transaction}
     *                     of the {@code UpdateSpec}.
     * @param settings     Array of {@code JobSetting} to apply to the newly created
     *                     JobRecord.
     * @param generatorJob The generator job or {@code null} if we are creating
     *                     the root job.
     * @param graphKey     The key of the child graph to which the new Job belongs
     *                     or {@code null} if we are creating the root job.
     * @param jobInstance  The user-supplied instance of {@code Job} that
     *                     implements the Job that the newly created JobRecord represents.
     * @param params       The arguments to be passed to the run() method of the newly
     *                     created Job. Each argument may be an actual value or it may be an
     *                     object of type {@link Value} representing either an
     *                     {@link ImmediateValue} or a
     *                     {@link com.google.appengine.tools.pipeline.FutureValue FutureValue}.
     *                     For each element of the array, if the Object is not of type
     *                     {@link Value} then it is interpreted as an {@link ImmediateValue}
     *                     with the given Object as its value.
     * @return The newly constructed JobRecord.
     */
    public JobRecord registerNewJobRecord(
            @Nonnull final UpdateSpec updateSpec,
            @Nonnull final UpdateSpec.Transaction transaction,
            @Nonnull final JobSetting[] settings,
            @Nonnull final JobRecord generatorJob,
            @Nonnull final UUID graphKey,
            @Nonnull final Job<?> jobInstance,
            @Nonnull final UUID assignedJobKey,
            @Nonnull final Object[] params
    ) {
        final JobRecord jobRecord = new JobRecord(this, generatorJob, graphKey, jobInstance, assignedJobKey, false, settings);
        injectJobMembers(jobInstance);
        return registerNewJobRecord(updateSpec, transaction, jobRecord, params);
    }

    public JobRecord registerNewJobRecord(final UpdateSpec updateSpec, final UpdateSpec.Transaction transaction, final JobRecord jobRecord, final Object[] params) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("registerNewJobRecord job(\"" + jobRecord + "\")");
        }

        final UUID generatorKey = jobRecord.getGeneratorJobKey();
        // Add slots to the RunBarrier corresponding to the input parameters
        final UUID graphKey = jobRecord.getGraphKey();
        for (final Object param : params) {
            final Value<?> value;
            if (null != param && param instanceof Value<?>) {
                value = (Value<?>) param;
            } else {
                value = new ImmediateValue<>(jobRecord.getPipelineKey(), param);
            }
            registerSlotsWithBarrier(
                    transaction,
                    updateSpec.getFinalTransaction(),
                    value,
                    value.getPipelineHandle(),
                    generatorKey,
                    jobRecord.getQueueSettings(),
                    graphKey,
                    jobRecord.getRunBarrierInflated()
            );
        }

        if (0 == jobRecord.getRunBarrierInflated().getWaitingOnKeys().size()) {
            // If the run barrier is not waiting on anything, add a phantom filled
            // slot in order to trigger a HandleSlotFilledTask in order to trigger
            // a RunJobTask.
            final Slot slot = new Slot(this, jobRecord.getPipelineKey(), generatorKey, graphKey);
            jobRecord.getRunBarrierInflated().addPhantomArgumentSlot(slot);
            registerSlotFilled(transaction, updateSpec.getFinalTransaction(), jobRecord.getQueueSettings(), slot, null);
        }

        // Register the newly created objects with the UpdateSpec.
        // The slots in the run Barrier have already been registered
        // and the finalize Barrier doesn't have any slots yet.
        // Any HandleSlotFilledTasks have also been registered already.
        transaction.includeBarrier(jobRecord.getRunBarrierInflated());
        transaction.includeBarrier(jobRecord.getFinalizeBarrierInflated());
        transaction.includeSlot(jobRecord.getOutputSlotInflated());
        transaction.includeJob(jobRecord);
        transaction.includeJobInstanceRecord(jobRecord.getJobInstanceInflated());

        return jobRecord;
    }

    private void injectJobMembers(final Job<?> jobInstance) {
        injector.injectMembers(jobInstance);
    }

    /**
     * Given a {@code Value} and a {@code Barrier}, we add one or more slots to
     * the waitingFor list of the barrier corresponding to the {@code Value}. We
     * also create {@link HandleSlotFilledTask HandleSlotFilledTasks} for all
     * filled slots. We register all newly created Slots and Tasks with the given
     * {@code UpdateSpec}.
     * <p>
     * If the value is an {@code ImmediateValue} we make a new slot, register it
     * as filled and add it. If the value is a {@code FutureValue} we add the slot
     * wrapped by the {@code FutreValueImpl}. If the value is a {@code FutureList}
     * then we add multiple slots, one for each {@code Value} in the List wrapped
     * by the {@code FutureList}. This process is not recursive because we do not
     * currently support {@code FutureLists} of {@code FutureLists}.
     *
     * @param transaction      All newly created Slots will be added to the
     *                         {@link UpdateSpec.Transaction} of the updateSpec.
     * @param finalTransaction All {@link HandleSlotFilledTask HandleSlotFilledTasks} created
     *                         will be added to the {@link UpdateSpec.TransactionWithTasks final
     *                         transaction}. Note that {@code barrier} will not be added
     *                         to updateSpec. That must be done by the caller.
     * @param value            A {@code Value}. {@code Null} is interpreted as an
     *                         {@code ImmediateValue} with a value of {@code Null}.
     * @param pipelineKey       The pipelineKey of the Pipeline in which the given Barrier
     *                         lives.
     * @param generatorJobKey  The key of the generator Job of the local graph in
     *                         which the given barrier lives, or {@code null} if the barrier lives
     *                         in the root Job graph.
     * @param queueSettings    The QueueSettings for tasks created by this method
     * @param graphKey         The key of the local graph in which the barrier lives, or
     *                         {@code null} if the barrier lives in the root Job graph.
     * @param barrier          The barrier to which we will add the slots
     */
    private void registerSlotsWithBarrier(
            final UpdateSpec.Transaction transaction,
            final UpdateSpec.TransactionWithTasks finalTransaction,
            final Value<?> value,
            final UUID pipelineKey,
            final UUID generatorJobKey,
            final QueueSettings queueSettings,
            final UUID graphKey,
            final Barrier barrier
    ) {
        if (null == value || value instanceof ImmediateValue<?>) {
            Object concreteValue = null;
            if (null != value) {
                final ImmediateValue<?> iv = (ImmediateValue<?>) value;
                concreteValue = iv.getValue();
            }
            final Slot slot = new Slot(this, pipelineKey, generatorJobKey, graphKey);
            registerSlotFilled(transaction, finalTransaction, queueSettings, slot, concreteValue);
            barrier.addRegularArgumentSlot(slot);
        } else if (value instanceof FutureValueImpl<?>) {
            final FutureValueImpl<?> futureValue = (FutureValueImpl<?>) value;
            final Slot slot = futureValue.getSlot();
            barrier.addRegularArgumentSlot(slot);
            transaction.includeSlot(slot);
        } else if (value instanceof FutureList<?>) {
            final FutureList<?> futureList = (FutureList<?>) value;
            final List<Slot> slotList = new ArrayList<>(futureList.getListOfValues().size());
            // The dummyListSlot is a marker slot that indicates that the
            // next group of slots forms a single list argument.
            final Slot dummyListSlot = new Slot(this, pipelineKey, generatorJobKey, graphKey);
            registerSlotFilled(transaction, finalTransaction, queueSettings, dummyListSlot, null);
            for (final Value<?> valFromList : futureList.getListOfValues()) {
                Slot slot = null;
                if (valFromList instanceof ImmediateValue<?>) {
                    final ImmediateValue<?> ivFromList = (ImmediateValue<?>) valFromList;
                    slot = new Slot(this, pipelineKey, generatorJobKey, graphKey);
                    registerSlotFilled(transaction, finalTransaction, queueSettings, slot, ivFromList.getValue());
                } else if (valFromList instanceof FutureValueImpl<?>) {
                    final FutureValueImpl<?> futureValFromList = (FutureValueImpl<?>) valFromList;
                    slot = futureValFromList.getSlot();
                } else if (value instanceof FutureList<?>) {
                    throw new IllegalArgumentException(
                            "The Pipeline framework does not currently support FutureLists of FutureLists");
                } else {
                    throwUnrecognizedValueException(valFromList);
                }
                slotList.add(slot);
                transaction.includeSlot(slot);
            }
            barrier.addListArgumentSlots(dummyListSlot, slotList);
        } else {
            throwUnrecognizedValueException(value);
        }
    }

    private void throwUnrecognizedValueException(final Value<?> value) {
        throw new RuntimeException(
                "Internal logic error: Unrecognized implementation of Value interface: "
                        + value.getClass().getName());
    }

    /**
     * Given a Slot and a concrete value with which to fill the slot, we fill the
     * value with the slot and create a new {@link HandleSlotFilledTask} for the
     * newly filled Slot. We register the Slot and the Task with the given
     * UpdateSpec for later saving.
     *
     * @param transaction      The Slot will be added to the {@link UpdateSpec.Transaction transaction}.
     * @param finalTransaction The new {@link HandleSlotFilledTask} will be added to
     *                         the {@link UpdateSpec.TransactionWithTasks final transaction}.
     * @param queueSettings    queue settings for the created task
     * @param slot             the Slot to fill
     * @param value            the value with which to fill it
     */
    private void registerSlotFilled(
            final UpdateSpec.Transaction transaction,
            final UpdateSpec.TransactionWithTasks finalTransaction,
            final QueueSettings queueSettings,
            final Slot slot,
            final Object value
    ) {
        slot.fill(value);
        transaction.includeSlot(slot);
        final Task task = new HandleSlotFilledTask(slot.getPipelineKey(), slot.getKey(), queueSettings);
        finalTransaction.registerTask(task);
    }

    /**
     * Returns all the associated PipelineModelObject for a root pipeline.
     *
     * @throws IllegalArgumentException if root pipeline was not found.
     */
    public PipelineObjects queryFullPipeline(final UUID pipelineKey) {
        return backEnd.queryFullPipeline(pipelineKey);
    }

    public List<PipelineRecord> queryRootPipelines(
            @Nullable final String classFilter,
            @Nullable final Set<JobRecord.State> inStates,
            final int limit,
            final int offset
    ) {
        return backEnd.queryRootPipelines(classFilter, inStates, limit, offset);
    }

    public Set<String> getRootPipelinesDisplayName() {
        return backEnd.getRootPipelinesDisplayName();
    }

    private void checkNonEmpty(final UUID s, final String name) {
        if (null == s) {
            throw new IllegalArgumentException(name + " is empty.");
        }
    }

    /**
     * Retrieves a JobRecord for the specified job handle. The returned instance
     * will be only partially inflated. The run and finalize barriers will not be
     * available but the output slot will be.
     *
     * @param pipelineKey
     * @param jobKey  The handle of a job.
     * @return The corresponding JobRecord
     * @throws NoSuchObjectException If a JobRecord with the given handle cannot
     *                               be found in the data store.
     */
    public JobRecord getJob(final UUID pipelineKey, final UUID jobKey) throws NoSuchObjectException {
        checkNonEmpty(jobKey, "jobKey");
        LOGGER.finest("getJob: " + jobKey);
        return backEnd.queryJob(pipelineKey, jobKey, JobRecord.InflationType.FOR_OUTPUT);
    }

    public PipelineRecord getPipeline(final UUID pipelineKey) throws NoSuchObjectException {
        checkNonEmpty(pipelineKey, "pipelineKey");
        LOGGER.finest("getPipeline: " + pipelineKey);
        return backEnd.queryPipeline(pipelineKey);
    }

    /**
     * Changes the state of the specified job to STOPPED.
     *
     * @param jobKey The handle of a job
     * @throws NoSuchObjectException If a JobRecord with the given handle cannot
     *                               be found in the data store.
     */
    public void stopJob(final UUID pipelineKey, final UUID jobKey) throws NoSuchObjectException {
        checkNonEmpty(jobKey, "jobKey");
        final JobRecord jobRecord = backEnd.queryJob(pipelineKey, jobKey, JobRecord.InflationType.NONE);
        jobRecord.setState(State.STOPPED);
        final UpdateSpec updateSpec = new UpdateSpec(jobRecord.getPipelineKey());
        updateSpec.newTransaction("stopJob").includeJob(jobRecord);
        backEnd.save(updateSpec, jobRecord.getQueueSettings());
    }

    /**
     * Sends cancellation request to the root job.
     *
     * @param jobKey The handle of a job
     * @throws NoSuchObjectException If a JobRecord with the given handle cannot
     *                               be found in the data store.
     */
    public void cancelJob(final UUID pipelineKey, final UUID jobKey) throws NoSuchObjectException {
        checkNonEmpty(jobKey, "jobKey");
        final JobRecord jobRecord = backEnd.queryJob(pipelineKey, jobKey, InflationType.NONE);
        final CancelJobTask cancelJobTask = new CancelJobTask(pipelineKey, jobKey, jobRecord.getQueueSettings());
        try {
            backEnd.enqueue(cancelJobTask);
        } catch (PipelineTaskQueue.TaskAlreadyExistsException ignore) {
            // OK. Some other thread has already enqueued this task.
        }
    }

    /**
     * Delete all data store entities corresponding to the given pipeline.
     *
     * @param pipelineHandle The handle of the pipeline to be deleted
     * @param force          If this parameter is not {@code true} then this method will
     *                       throw an {@link IllegalStateException} if the specified pipeline is
     *                       not in the {@link State#FINALIZED} or {@link State#STOPPED} state.
     * @param async          If this parameter is {@code true} then instead of performing
     *                       the delete operation synchronously, this method will enqueue a task
     *                       to perform the operation.
     * @throws NoSuchObjectException If there is no Job with the given key.
     * @throws IllegalStateException If {@code force = false} and the specified
     *                               pipeline is not in the {@link State#FINALIZED} or
     *                               {@link State#STOPPED} state.
     */
    public void deletePipelineRecords(final UUID pipelineHandle, final boolean force, final boolean async)
            throws NoSuchObjectException, IllegalStateException {
        checkNonEmpty(pipelineHandle, "pipelineHandle");
        backEnd.deletePipeline(pipelineHandle, force, async);
    }

    public void acceptPromisedValue(final UUID pipelineKey, final UUID promiseHandle, final Object value)
            throws NoSuchObjectException, OrphanedObjectException {
        checkNonEmpty(promiseHandle, "promiseHandle");
        Slot slot = null;
        // It is possible, though unlikely, that we might be asked to accept a
        // promise before the slot to hold the promise has been saved. We will try 5
        // times, sleeping 1, 2, 4, 8 seconds between attempts.
        int attempts = 0;
        boolean interrupted = false;
        try {
            while (slot == null) {
                attempts++;
                try {
                    slot = backEnd.querySlot(pipelineKey, promiseHandle, false);
                } catch (NoSuchObjectException e) {
                    if (attempts >= 5) {
                        throw new NoSuchObjectException("There is no promise with handle " + promiseHandle);
                    }
                    try {
                        Thread.sleep(Duration.ofSeconds((long) Math.pow(2.0, attempts - 1)).toMillis());
                    } catch (InterruptedException ex) {
                        interrupted = true;
                    }
                }
            }
        } finally {
            // TODO(user): replace with Uninterruptibles#sleepUninterruptibly once we use guava
            if (interrupted) {

                Thread.currentThread().interrupt();
            }
        }
        final UUID generatorJobKey = slot.getGeneratorJobKey();
        if (null == generatorJobKey) {
            throw new RuntimeException(
                    "Pipeline is fatally corrupted. Slot for promised value has no generatorJobKey: " + slot);
        }
        final JobRecord generatorJob = backEnd.queryJob(pipelineKey, generatorJobKey, JobRecord.InflationType.NONE);
        if (null == generatorJob) {
            throw new RuntimeException("Pipeline is fatally corrupted. "
                    + "The generator job for a promised value slot was not found: " + generatorJobKey);
        }
        final UUID childGraphKey = generatorJob.getChildGraphKey();
        if (null == childGraphKey) {
            // The generator job has not been saved with a childGraphKey yet. This can happen if the
            // promise handle leaked out to an external thread before the job that generated it
            // had finished.
            throw new NoSuchObjectException(
                    "The framework is not ready to accept the promised value yet. "
                            + "Please try again after the job that generated the promis handle has completed.");
        }
        if (!childGraphKey.equals(slot.getGraphKey())) {
            // The slot has been orphaned
            throw new OrphanedObjectException(promiseHandle);
        }
        final UpdateSpec updateSpec = new UpdateSpec(slot.getPipelineKey());
        registerSlotFilled(updateSpec.newTransaction("acceptPromisedValue:" + promiseHandle), updateSpec.getFinalTransaction(), generatorJob.getQueueSettings(), slot, value);
        backEnd.save(updateSpec, generatorJob.getQueueSettings());
    }

    public void shutdown() {
        backEnd.shutdown();
    }

    /**
     * Process an incoming task received from the App Engine task queue.
     *
     * @param task The task to be processed.
     */
    public void processTask(final Task task) {
        LOGGER.finest("Processing task " + task);
        try {
            switch (task.getType()) {
                case RUN_JOB:
                    runJob((RunJobTask) task);
                    break;
                case HANDLE_SLOT_FILLED:
                    handleSlotFilled((HandleSlotFilledTask) task);
                    break;
                case FINALIZE_JOB:
                    finalizeJob((FinalizeJobTask) task);
                    break;
                case FAN_OUT:
                    handleFanoutTaskOrAbandonTask((FanoutTask) task);
                    break;
                case CANCEL_JOB:
                    cancelJobInternal((CancelJobTask) task);
                    break;
                case DELETE_PIPELINE:
                    final DeletePipelineTask deletePipelineTask = (DeletePipelineTask) task;
                    try {
                        backEnd.deletePipeline(
                                deletePipelineTask.getPipelineKey(), deletePipelineTask.shouldForce(), false);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "DeletePipeline operation failed.", e);
                    }
                    break;
                case HANDLE_CHILD_EXCEPTION:
                    handleChildException((HandleChildExceptionTask) task);
                    break;
                case DELAYED_SLOT_FILL:
                    handleDelayedSlotFill((DelayedSlotFillTask) task);
                    break;
                default:
                    throw new IllegalArgumentException("Unrecognized task type: " + task.getType());
            }
        } catch (AbandonTaskException ignore) {
            // return 200;
        }
    }

    public PipelineBackEnd getBackEnd() {
        return backEnd;
    }

    public PipelineTaskQueue getTaskQueue() {
        return taskQueue;
    }

    private void invokePrivateJobMethod(final String methodName, final Job<?> job, final Object... params) {
        final Class<?>[] signature = new Class<?>[params.length];
        int i = 0;
        for (final Object param : params) {
            signature[i++] = param.getClass();
        }
        invokePrivateJobMethod(methodName, job, signature, params);
    }

    private void invokePrivateJobMethod(
            final String methodName, final Job<?> job, final Class<?>[] signature, final Object... params) {
        try {
            final Method method = Job.class.getDeclaredMethod(methodName, signature);
            method.setAccessible(true);
            method.invoke(job, params);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return handleException method (if callErrorHandler is {@code true}).
     * Otherwise return first run method (order is defined by order of
     * {@link Class#getMethods()} is return. <br>
     * TODO(user) Consider actually looking for a method with matching
     * signature<br>
     * TODO(maximf) Consider getting rid of reflection based invocations
     * completely.
     *
     * @param klass            Class of the user provided job
     * @param callErrorHandler should handleException method be returned instead
     *                         of run
     * @param params           parameters to be passed to the method to invoke
     * @return Either run or handleException method of {@code class}. {@code null
     * } is returned if @{code callErrorHandler} is {@code true}
     * and no handleException with matching signature is found.
     */
    @SuppressWarnings("unchecked")
    private Method findJobMethodToInvoke(
            final Class<?> klass, final boolean callErrorHandler, final Object[] params) {
        Method runMethod = null;
        if (callErrorHandler) {
            if (params.length != 1) {
                throw new RuntimeException(
                        "Invalid number of parameters passed to handleException:" + params.length);
            }
            final Object parameter = params[0];
            if (parameter == null) {
                throw new RuntimeException("Null parameters passed to handleException:" + params.length);
            }
            final Class<?> parameterClass = parameter.getClass();
            if (!Throwable.class.isAssignableFrom(parameterClass)) {
                throw new RuntimeException("Parameter that is not an exception passed to handleException:"
                        + parameterClass.getName());
            }
            Class<? extends Throwable> exceptionClass = (Class<? extends Throwable>) parameterClass;
            do {
                try {
                    runMethod = klass.getMethod(
                            JobRecord.EXCEPTION_HANDLER_METHOD_NAME, exceptionClass);
                    break;
                } catch (NoSuchMethodException ignore) {
                    // Ignore, try parent instead.
                }
                exceptionClass = (Class<? extends Throwable>) exceptionClass.getSuperclass();
            } while (exceptionClass != null);
        } else {
            for (final Method method : klass.getMethods()) {
                if ("run".equals(method.getName())) {
                    runMethod = method;
                    break;
                }
            }
        }
        return runMethod;
    }

    private void setJobRecord(final Job<?> job, final JobRecord jobRecord) {
        invokePrivateJobMethod("setJobRecord", job, jobRecord);
    }

    private void setCurrentRunKey(final Job<?> job, final UUID key) {
        invokePrivateJobMethod("setCurrentRunKey", job, key);
    }

    private void setUpdateSpec(final Job<?> job, final UpdateSpec updateSpec) {
        invokePrivateJobMethod("setUpdateSpec", job, updateSpec);
    }

    private void setChildValues(final Job<?> job, final List<FutureValue<?>> childValues) {
        invokePrivateJobMethod("setChildValues", job, childValues);
    }

    /**
     * Run the job with the given key.
     * <p>
     * We fetch the {@link JobRecord} from the data store and then fetch its run
     * {@link Barrier} and all of the {@link Slot Slots} in the run
     * {@code Barrier} (which should be filled.) We use the values of the filled
     * {@code Slots} to populate the arguments of the run() method of the
     * {@link Job} instance associated with the job and we invoke the
     * {@code run()} method. We save any generated child job graph and we enqueue
     * a {@link HandleSlotFilledTask} for any slots that are filled immediately.
     *
     * @see "http://goto/java-pipeline-model"
     */
    private void runJob(final RunJobTask task) {
        final UUID pipelineKey = task.getPipelineKey();
        final UUID jobKey = task.getJobKey();
        final JobRecord jobRecord = queryJobOrAbandonTask(pipelineKey, jobKey, JobRecord.InflationType.FOR_RUN);
        jobRecord.getQueueSettings().merge(task.getQueueSettings());
        LOGGER.info("Running pipeline job " + jobKey + "; UI at "
                + PipelineServlet.makeViewerUrl(pipelineKey, jobKey));
        final JobRecord rootJobRecord;
        if (pipelineKey.equals(jobKey)) {
            rootJobRecord = jobRecord;
        } else {
            rootJobRecord = queryJobOrAbandonTask(pipelineKey, pipelineKey, JobRecord.InflationType.NONE);
        }
        if (rootJobRecord.getState() == State.STOPPED) {
            LOGGER.warning("The pipeline has been stopped: " + rootJobRecord);
            throw new AbandonTaskException();
        }

        // TODO(user): b/12301978, check if its a previous run and if so AbandonTaskException
        final Barrier runBarrier = jobRecord.getRunBarrierInflated();
        if (null == runBarrier) {
            throw new RuntimeException("Internal logic error: " + jobRecord + " has not been inflated.");
        }
        final Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
        if (null == finalizeBarrier) {
            throw new RuntimeException(
                    "Internal logic error: finalize barrier not inflated in " + jobRecord);
        }

        // Release the run barrier now so any concurrent HandleSlotFilled tasks
        // will stop trying to release it.
        runBarrier.setReleased();
        UpdateSpec tempSpec = new UpdateSpec(pipelineKey);
        tempSpec.newTransaction("runJob:releaseRunBarrier:" + runBarrier.getKey()).includeBarrier(runBarrier);
        backEnd.save(tempSpec, jobRecord.getQueueSettings());

        final State jobState = jobRecord.getState();
        switch (jobState) {
            case WAITING_TO_RUN:
            case RETRY:
                // OK, proceed
                break;
            case WAITING_TO_FINALIZE:
                LOGGER.info("This job has already been run " + jobRecord);
                return;
            case STOPPED:
                LOGGER.info("This job has been stoped " + jobRecord);
                return;
            case CANCELED:
                LOGGER.info("This job has already been canceled " + jobRecord);
                return;
            case FINALIZED:
                LOGGER.info("This job has already been run " + jobRecord);
                return;
            default:
                LOGGER.info("Unknown state " + jobRecord);
        }

        // Deserialize the instance of Job and set some values on the instance
        final JobInstanceRecord record = jobRecord.getJobInstanceInflated();
        if (null == record) {
            throw new RuntimeException(
                    "Internal logic error:" + jobRecord + " does not have jobInstanceInflated.");
        }
        final Job<?> job = record.getJobInstanceDeserialized();
        final UpdateSpec updateSpec = new UpdateSpec(pipelineKey);
        setJobRecord(job, jobRecord);
        final UUID currentRunKey = UuidGenerator.nextUuid();
        setCurrentRunKey(job, currentRunKey);
        setUpdateSpec(job, updateSpec);
        final List<FutureValue<?>> childValues = Lists.newArrayList();
        setChildValues(job, childValues);

        injectJobMembers(job);

        // Get the run() method we will invoke and its arguments
        final Object[] params = runBarrier.buildArgumentArray();
        final boolean callExceptionHandler = jobRecord.isCallExceptionHandler();
        final Method methodToExecute =
                findJobMethodToInvoke(job.getClass(), callExceptionHandler, params);
        if (callExceptionHandler && methodToExecute == null) {
            // No matching exceptionHandler found. Propagate to the parent.
            final Throwable exceptionToHandle = (Throwable) params[0];
            handleExceptionDuringRun(jobRecord, rootJobRecord, currentRunKey, exceptionToHandle);
            return;
        }
        if (LOGGER.isLoggable(Level.FINEST)) {
            final StringBuilder builder = new StringBuilder(1024);
            builder.append("Running " + jobRecord + " with params: ");
            builder.append(StringUtils.toString(params));
            LOGGER.finest(builder.toString());
        }

        // Set the Job's start time and save the jobRecord now before we invoke
        // run(). The start time will be displayed in the UI.
        jobRecord.incrementAttemptNumber();
        jobRecord.setStartTime(new Date());
        tempSpec = new UpdateSpec(jobRecord.getPipelineKey());
        tempSpec.newTransaction("runJob:setStartTime:" + jobRecord.getKey()).includeJob(jobRecord);
        if (!backEnd.saveWithJobStateCheck(
                tempSpec, jobRecord.getQueueSettings(), jobKey, State.WAITING_TO_RUN, State.RETRY)) {
            LOGGER.info("Ignoring runJob request for job " + jobRecord + " which is not in a"
                    + " WAITING_TO_RUN or a RETRY state");
            return;
        }
        //TODO(user): Use the commented code to avoid resetting stack trace of rethrown exceptions
        //    List<Throwable> throwableParams = new ArrayList<Throwable>(params.length);
        //    for (Object param : params) {
        //      if (param instanceof Throwable) {
        //        throwableParams.add((Throwable) param);
        //      }
        //    }
        // Invoke the run or handleException method. This has the side-effect of populating
        // the UpdateSpec with any child job graph generated by the invoked method.
        Value<?> returnValue = null;
        Throwable caughtException = null;
        try {
            methodToExecute.setAccessible(true);
            returnValue = (Value<?>) methodToExecute.invoke(job, params);
        } catch (InvocationTargetException e) {
            caughtException = e.getCause();
        } catch (Throwable e) {
            caughtException = e;
        }
        if (null != caughtException) {
            //TODO(user): use the following condition to keep original exception trace
            //      if (!throwableParams.contains(caughtException)) {
            //      }
            handleExceptionDuringRun(jobRecord, rootJobRecord, currentRunKey, caughtException);
            return;
        }

        // The run() method returned without error.
        // We do all of the following in a transaction:
        // (1) Check that the job is currently in the state WAITING_TO_RUN or RETRY
        // (2) Change the state of the job to WAITING_TO_FINALIZE
        // (3) Set the finalize slot to be the one generated by the run() method
        // (4) Set the job's child graph key to be the currentRunKey
        // (5) Enqueue a FanoutTask that will fan-out to a set of
        // HandleSlotFilledTasks for each of the slots that were immediately filled
        // by the running of the job.
        // See "http://goto/java-pipeline-model".
        LOGGER.finest("Job returned: " + returnValue);
        registerSlotsWithBarrier(
                updateSpec.getFinalTransaction(),
                updateSpec.getFinalTransaction(),
                returnValue,
                pipelineKey,
                jobRecord.getKey(),
                jobRecord.getQueueSettings(),
                currentRunKey,
                finalizeBarrier
        );
        // adding all the children FutureValues to finalize barrier to automatically wait util all of them are finished
        if (!childValues.isEmpty()) {
            for (final FutureValue<?> childValue : childValues) {
                registerSlotsWithBarrier(
                        updateSpec.getFinalTransaction(),
                        updateSpec.getFinalTransaction(),
                        childValue,
                        pipelineKey,
                        jobRecord.getKey(),
                        jobRecord.getQueueSettings(),
                        currentRunKey,
                        finalizeBarrier
                );
            }
        }
        jobRecord.setState(State.WAITING_TO_FINALIZE);
        jobRecord.setChildGraphKey(currentRunKey);
        updateSpec.getFinalTransaction().includeJob(jobRecord);
        updateSpec.getFinalTransaction().includeBarrier(finalizeBarrier);
        backEnd.saveWithJobStateCheck(
                updateSpec, jobRecord.getQueueSettings(), jobKey, State.WAITING_TO_RUN, State.RETRY);
    }

    private void cancelJobInternal(final CancelJobTask cancelJobTask) {
        final UUID pipelineKey = cancelJobTask.getPipelineKey();
        final UUID jobKey = cancelJobTask.getJobKey();
        final JobRecord jobRecord = queryJobOrAbandonTask(pipelineKey, jobKey, JobRecord.InflationType.FOR_RUN);
        jobRecord.getQueueSettings().merge(cancelJobTask.getQueueSettings());
        LOGGER.info("Cancelling pipeline job " + jobKey);
        final JobRecord rootJobRecord;
        if (pipelineKey.equals(jobKey)) {
            rootJobRecord = jobRecord;
        } else {
            rootJobRecord = queryJobOrAbandonTask(pipelineKey, pipelineKey, JobRecord.InflationType.NONE);
        }
        if (rootJobRecord.getState() == State.STOPPED) {
            LOGGER.warning("The pipeline has been stopped: " + rootJobRecord);
            throw new AbandonTaskException();
        }

        switch (jobRecord.getState()) {
            case WAITING_TO_RUN:
            case RETRY:
            case WAITING_TO_FINALIZE:
                // OK, proceed
                break;
            case STOPPED:
                LOGGER.info("This job has been stoped " + jobRecord);
                return;
            case CANCELED:
                LOGGER.info("This job has already been canceled " + jobRecord);
                return;
            case FINALIZED:
                LOGGER.info("This job has already been run " + jobRecord);
                return;
            default:
                LOGGER.info("Unknown state " + jobRecord);
                return;
        }

        if (jobRecord.getChildKeys().size() > 0) {
            cancelChildren(jobRecord, null);
        }

        // No error handler present. So just mark this job as CANCELED.
        if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.finest("Marking " + jobRecord + " as CANCELED");
        }
        jobRecord.setState(State.CANCELED);
        final UpdateSpec updateSpec = new UpdateSpec(jobRecord.getPipelineKey());
        updateSpec.newTransaction("cancelJob:" + jobKey).includeJob(jobRecord);
        if (jobRecord.isExceptionHandlerSpecified()) {
            executeExceptionHandler(updateSpec, jobRecord, new CancellationException(), true);
        }
        backEnd.save(updateSpec, jobRecord.getQueueSettings());
    }

    private void handleExceptionDuringRun(final JobRecord jobRecord, final JobRecord rootJobRecord,
                                          final UUID currentRunKey, final Throwable caughtException) {
        final int attemptNumber = jobRecord.getAttemptNumber();
        final int maxAttempts = jobRecord.getMaxAttempts();
        if (jobRecord.isCallExceptionHandler()) {
            LOGGER.log(Level.INFO,
                    "An exception occurred when attempting to execute exception hander job " + jobRecord
                            + ". ", caughtException);
        } else {
            LOGGER.log(Level.INFO, "An exception occurred when attempting to run " + jobRecord + ". "
                            + "This was attempt number " + attemptNumber + " of " + maxAttempts + ".",
                    caughtException);
        }
        if (jobRecord.isIgnoreException()) {
            return;
        }
        final UpdateSpec updateSpec = new UpdateSpec(jobRecord.getPipelineKey());
        final ExceptionRecord exceptionRecord = new ExceptionRecord(
                this,
                jobRecord.getPipelineKey(),
                jobRecord.getKey(),
                currentRunKey,
                caughtException
        );
        final UpdateSpec.Transaction updateGroup = updateSpec.newTransaction("handleExceptionDuringRun:" + jobRecord.getKey());
        updateGroup.includeException(exceptionRecord);
        final UUID exceptionKey = exceptionRecord.getKey();
        jobRecord.setExceptionKey(exceptionKey);
        if (jobRecord.isCallExceptionHandler() || attemptNumber >= maxAttempts) {
            jobRecord.setState(State.STOPPED);
            updateGroup.includeJob(jobRecord);
            if (jobRecord.isExceptionHandlerSpecified()) {
                cancelChildren(jobRecord, null);
                executeExceptionHandler(updateSpec, jobRecord, caughtException, false);
            } else {
                if (null != jobRecord.getExceptionHandlingAncestorKey()) {
                    cancelChildren(jobRecord, null);
                    // current job doesn't have an error handler. So just delegate it to the
                    // nearest ancestor that has one.
                    final Task handleChildExceptionTask = new HandleChildExceptionTask(
                            jobRecord.getPipelineKey(),
                            jobRecord.getExceptionHandlingAncestorKey(),
                            jobRecord.getKey(),
                            jobRecord.getQueueSettings());
                    updateSpec.getFinalTransaction().registerTask(handleChildExceptionTask);
                } else {
                    rootJobRecord.setState(State.STOPPED);
                    rootJobRecord.setExceptionKey(exceptionKey);
                    updateGroup.includeJob(rootJobRecord);
                }
            }
            backEnd.save(updateSpec, jobRecord.getQueueSettings());
        } else {
            jobRecord.setState(State.RETRY);
            final int backoffFactor = jobRecord.getBackoffFactor();
            final int backoffSeconds = jobRecord.getBackoffSeconds();
            final RunJobTask task =
                    new RunJobTask(jobRecord.getPipelineKey(), jobRecord.getKey(), attemptNumber, jobRecord.getQueueSettings());
            task.getQueueSettings().setDelayInSeconds(
                    backoffSeconds * (long) Math.pow(backoffFactor, attemptNumber));
            updateSpec.getFinalTransaction().includeJob(jobRecord);
            updateSpec.getFinalTransaction().registerTask(task);
            backEnd.saveWithJobStateCheck(updateSpec, jobRecord.getQueueSettings(), jobRecord.getKey(),
                    State.WAITING_TO_RUN, State.RETRY);
        }
    }

    /**
     * @param updateSpec      UpdateSpec to use
     * @param jobRecord       record of the job with exception handler to execute
     * @param caughtException failure cause
     * @param ignoreException if failure should be ignored (used for cancellation)
     */
    private void executeExceptionHandler(final UpdateSpec updateSpec, final JobRecord jobRecord,
                                         final Throwable caughtException, final boolean ignoreException) {
        final UpdateSpec.Transaction transaction = updateSpec.newTransaction("executeExceptionHandler:" + jobRecord.getKey());
        transaction.includeJob(jobRecord);
        final UUID errorHandlingGraphKey = UuidGenerator.nextUuid();
        final Job<?> jobInstance = jobRecord.getJobInstanceInflated().getJobInstanceDeserialized();

        final JobRecord errorHandlingJobRecord = new JobRecord(this, jobRecord, errorHandlingGraphKey, jobInstance, UuidGenerator.nextUuid(), true, new JobSetting[0]);
        errorHandlingJobRecord.setOutputSlotInflated(jobRecord.getOutputSlotInflated());
        errorHandlingJobRecord.setIgnoreException(ignoreException);
        registerNewJobRecord(updateSpec, transaction, errorHandlingJobRecord, new Object[]{new ImmediateValue<>(jobRecord.getPipelineKey(), caughtException)});
    }

    private void cancelChildren(final JobRecord jobRecord, final UUID failedChildKey) {
        for (final UUID childKey : jobRecord.getChildKeys()) {
            if (!childKey.equals(failedChildKey)) {
                final CancelJobTask cancelJobTask = new CancelJobTask(jobRecord.getPipelineKey(), childKey, jobRecord.getQueueSettings());
                try {
                    backEnd.enqueue(cancelJobTask);
                } catch (PipelineTaskQueue.TaskAlreadyExistsException ignore) {
                    // OK. Some other thread has already enqueued this task.
                }
            }
        }
    }

    private void handleChildException(final HandleChildExceptionTask handleChildExceptionTask) {
        final UUID pipelineKey = handleChildExceptionTask.getPipelineKey();
        final UUID jobKey = handleChildExceptionTask.getKey();
        final UUID failedChildKey = handleChildExceptionTask.getFailedChildKey();
        final JobRecord jobRecord = queryJobOrAbandonTask(pipelineKey, jobKey, JobRecord.InflationType.FOR_RUN);
        jobRecord.getQueueSettings().merge(handleChildExceptionTask.getQueueSettings());
        LOGGER.info("Running pipeline job " + jobKey + " exception handler; UI at "
                + PipelineServlet.makeViewerUrl(pipelineKey, jobKey));
        final JobRecord rootJobRecord;
        if (pipelineKey.equals(jobKey)) {
            rootJobRecord = jobRecord;
        } else {
            rootJobRecord = queryJobOrAbandonTask(pipelineKey, pipelineKey, JobRecord.InflationType.NONE);
        }
        if (rootJobRecord.getState() == State.STOPPED) {
            LOGGER.warning("The pipeline has been stopped: " + rootJobRecord);
            throw new AbandonTaskException();
        }
        // TODO(user): add jobState check
        final JobRecord failedJobRecord =
                queryJobOrAbandonTask(pipelineKey, failedChildKey, JobRecord.InflationType.FOR_OUTPUT);
        final UpdateSpec updateSpec = new UpdateSpec(pipelineKey);
        cancelChildren(jobRecord, failedChildKey);
        executeExceptionHandler(updateSpec, jobRecord, failedJobRecord.getException(), false);
        backEnd.save(updateSpec, jobRecord.getQueueSettings());
    }

    /**
     * Finalize the job with the given key.
     * <p>
     * We fetch the {@link JobRecord} from the data store and then fetch its
     * finalize {@link Barrier} and all of the {@link Slot Slots} in the finalize
     * {@code Barrier} (which should be filled.) We set the finalize Barrier to
     * released and save it. We fetch the job's output Slot. We use the values of
     * the filled finalize {@code Slots} to populate the output slot. We set the
     * state of the Job to {@code FINALIZED} and save the {@link JobRecord} and
     * the output slot. Finally we enqueue a {@link HandleSlotFilledTask} for the
     * output slot.
     *
     * @see "http://goto/java-pipeline-model"
     */
    private void finalizeJob(final FinalizeJobTask finalizeJobTask) {
        final UUID jobKey = finalizeJobTask.getJobKey();
        // Get the JobRecord, its finalize Barrier, all the slots in the
        // finalize Barrier, and the job's output Slot.
        final JobRecord jobRecord = queryJobOrAbandonTask(finalizeJobTask.getPipelineKey(), jobKey, JobRecord.InflationType.FOR_FINALIZE);
        jobRecord.getQueueSettings().merge(finalizeJobTask.getQueueSettings());
        switch (jobRecord.getState()) {
            case WAITING_TO_FINALIZE:
                // OK, proceed
                break;
            case WAITING_TO_RUN:
            case RETRY:
                throw new RuntimeException("" + jobRecord + " is in RETRY state");
            case STOPPED:
                LOGGER.info("This job has been stoped " + jobRecord);
                return;
            case CANCELED:
                LOGGER.info("This job has already been canceled " + jobRecord);
                return;
            case FINALIZED:
                LOGGER.info("This job has already been run " + jobRecord);
                return;
            default:
                LOGGER.info("Unknown state " + jobRecord);
                return;
        }
        final Barrier finalizeBarrier = jobRecord.getFinalizeBarrierInflated();
        if (null == finalizeBarrier) {
            throw new RuntimeException("" + jobRecord + " has not been inflated");
        }
        final Slot outputSlot = jobRecord.getOutputSlotInflated();
        if (null == outputSlot) {
            throw new RuntimeException("" + jobRecord + " has not been inflated.");
        }

        // release the finalize barrier now so that any concurrent
        // HandleSlotFilled tasks will stop trying
        finalizeBarrier.setReleased();
        UpdateSpec updateSpec = new UpdateSpec(jobRecord.getPipelineKey());
        updateSpec.newTransaction("finalizeJob:releaseFinalizeBarrier:" + jobRecord.getKey()).includeBarrier(finalizeBarrier);
        backEnd.save(updateSpec, jobRecord.getQueueSettings());

        updateSpec = new UpdateSpec(jobRecord.getPipelineKey());
        // Copy the finalize value to the output slot
        final List<Object> finalizeArguments = finalizeBarrier.buildArgumentList();
        final int numFinalizeArguments = finalizeArguments.size();
        if (numFinalizeArguments == 0) {
            // 0-th argument is the actual value that job returned.
            // 1-st and following are results of child jobs that were used to block finalize until they all are finished
            throw new RuntimeException(
                    "Internal logic error: numFinalizeArguments=" + numFinalizeArguments);
        }
        final Object finalizeValue = finalizeArguments.get(0);
        LOGGER.finest("Finalizing " + jobRecord + " with value=" + finalizeValue);
        outputSlot.fill(finalizeValue);

        // Change state of the job to FINALIZED and set the end time
        jobRecord.setState(State.FINALIZED);
        jobRecord.setEndTime(new Date());

        // Propagate the filler of the finalize slot to also be the filler of the
        // output slot. If there is no unique filler of the finalize slot then we
        // resort to assigning the current job as the filler job.
        UUID fillerJobKey = getFinalizeSlotFiller(finalizeBarrier);
        if (null == fillerJobKey) {
            fillerJobKey = jobKey;
        }
        outputSlot.setSourceJobKey(fillerJobKey);

        // Save the job and the output slot
        final UpdateSpec.Transaction updateGroup = updateSpec.newTransaction("finalizeJob:jobAndSlot:" + jobKey);
        updateGroup.includeJob(jobRecord);
        updateGroup.includeSlot(outputSlot);
        backEnd.save(updateSpec, jobRecord.getQueueSettings());

        // enqueue a HandleSlotFilled task
        final HandleSlotFilledTask task =
                new HandleSlotFilledTask(outputSlot.getPipelineKey(), outputSlot.getKey(), jobRecord.getQueueSettings());
        backEnd.enqueue(task);
    }

    /**
     * Return the unique value of {@link Slot#getSourceJobKey()} for all slots in
     * the finalize Barrier, if there is a unique such value. Otherwise return
     * {@code null}.
     *
     * @param finalizeBarrier A finalize Barrier
     * @return The unique slot filler for the finalize slots, or {@code null}
     */
    private UUID getFinalizeSlotFiller(final Barrier finalizeBarrier) {
        UUID fillerJobKey = null;
        for (final SlotDescriptor slotDescriptor : finalizeBarrier.getWaitingOnInflated()) {
            final UUID key = slotDescriptor.getSlot().getSourceJobKey();
            if (null != key) {
                if (null == fillerJobKey) {
                    fillerJobKey = key;
                } else {
                    if (!fillerJobKey.toString().equals(key.toString())) {
                        // Found 2 non-equal values, return null
                        return null;
                    }
                }
            }
        }
        return fillerJobKey;
    }

    /**
     * Handle the fact that the slot with the given key has been filled.
     * <p>
     * For each barrier that is waiting on the slot, if all of the slots that the
     * barrier is waiting on are now filled then the barrier should be released.
     * Release the barrier by enqueueing an appropriate task (either
     * {@link RunJobTask} or {@link FinalizeJobTask}.
     */
    private void handleSlotFilled(final HandleSlotFilledTask hsfTask) {
        final UUID pipelineKey = hsfTask.getPipelineKey();
        final UUID slotKey = hsfTask.getSlotKey();
        final Slot slot = querySlotOrAbandonTask(pipelineKey, slotKey, true);
        final List<Barrier> waitingList = slot.getWaitingOnMeInflated();
        if (null == waitingList) {
            throw new RuntimeException("Internal logic error: " + slot + " is not inflated");
        }
        // For each barrier that is waiting on the slot ...
        for (final Barrier barrier : waitingList) {
            LOGGER.finest("Checking " + barrier);
            // unless the barrier has already been released,
            if (!barrier.isReleased()) {
                // we check whether the barrier should be released.
                boolean shouldBeReleased = true;
                if (null == barrier.getWaitingOnInflated()) {
                    throw new RuntimeException("Internal logic error: " + barrier + " is not inflated.");
                }
                // For each slot that the barrier is waiting on...
                for (final SlotDescriptor sd : barrier.getWaitingOnInflated()) {
                    // see if it is full.
                    if (!sd.getSlot().isFilled()) {
                        LOGGER.finest("Not filled: " + sd.getSlot());
                        shouldBeReleased = false;
                        break;
                    }
                }
                if (shouldBeReleased) {
                    final JobRecord jobRecord = queryJobOrAbandonTask(barrier.getPipelineKey(), barrier.getJobKey(), JobRecord.InflationType.NONE);
                    jobRecord.getQueueSettings().merge(hsfTask.getQueueSettings());
                    final Task task;
                    switch (barrier.getType()) {
                        case RUN:
                            task = new RunJobTask(barrier.getPipelineKey(), barrier.getJobKey(), jobRecord.getQueueSettings());
                            break;
                        case FINALIZE:
                            task = new FinalizeJobTask(barrier.getPipelineKey(), barrier.getJobKey(), jobRecord.getQueueSettings());
                            break;
                        default:
                            throw new RuntimeException("Unknown barrier type " + barrier.getType());
                    }
                    try {
                        backEnd.enqueue(task);
                    } catch (PipelineTaskQueue.TaskAlreadyExistsException ignore) {
                        // OK. Some other thread has already enqueued this task.
                    }
                }
            }
        }
    }

    /**
     * Fills the slot with null value and calls handleSlotFilled
     */
    private void handleDelayedSlotFill(final DelayedSlotFillTask task) {
        final UUID pipelineKey = task.getPipelineKey();
        final UUID slotKey = task.getSlotKey();
        final Slot slot = querySlotOrAbandonTask(pipelineKey, slotKey, true);
        final UpdateSpec updateSpec = new UpdateSpec(pipelineKey);
        slot.fill(null);
        updateSpec.newTransaction("handleDelayedSlotFill:" + slotKey).includeSlot(slot);
        backEnd.save(updateSpec, task.getQueueSettings());
        // re-reading Slot (in handleSlotFilled) is needed (to capture slot fill after this one)
        handleSlotFilled(new HandleSlotFilledTask(pipelineKey, slotKey, task.getQueueSettings()));
    }

    /**
     * Queries for the job with the given key from the data store and if the job
     * is not found then throws an {@link AbandonTaskException}.
     *
     * @param key           The key of the JobRecord to be fetched
     * @param inflationType Specifies the manner in which the returned JobRecord
     *                      should be inflated.
     * @return A {@code JobRecord}, possibly with a partially-inflated associated
     * graph of objects.
     * @throws AbandonTaskException If Either the JobRecord or any of the
     *                              associated Slots or Barriers are not found in the data store.
     */
    private JobRecord queryJobOrAbandonTask(final UUID pipelineKey, final UUID key, final JobRecord.InflationType inflationType) {
        try {
            return backEnd.queryJob(pipelineKey, key, inflationType);
        } catch (NoSuchObjectException e) {
            LOGGER.log(
                    Level.WARNING, "Cannot find some part of the job: " + key + ". Ignoring the task.", e);
            throw new AbandonTaskException();
        }
    }

    /**
     * Queries the Slot with the given Key from the data store and if the Slot is
     * not found then throws an {@link AbandonTaskException}.
     *
     * @param key     The Key of the slot to fetch.
     * @param inflate If this is {@code true} then the Barriers that are waiting
     *                on the Slot and the other Slots that those Barriers are waiting on
     *                will also be fetched from the data store and used to partially
     *                populate the graph of objects attached to the returned Slot. In
     *                particular: {@link Slot#getWaitingOnMeInflated()} will not return
     *                {@code null} and also that for each of the {@link Barrier Barriers}
     *                returned from that method {@link Barrier#getWaitingOnInflated()}
     *                will not return {@code null}.
     * @return A {@code Slot}, possibly with a partially-inflated associated graph
     * of objects.
     * @throws AbandonTaskException If either the Slot or the associated Barriers
     *                              and slots are not found in the data store.
     */
    private Slot querySlotOrAbandonTask(final UUID pipelineKey, final UUID key, final boolean inflate) {
        try {
            return backEnd.querySlot(pipelineKey, key, inflate);
        } catch (NoSuchObjectException e) {
            LOGGER.log(Level.WARNING, "Cannot find the slot: " + key + ". Ignoring the task.", e);
            throw new AbandonTaskException();
        }
    }

    /**
     * Handles the given FanoutTask and if the corresponding FanoutTaskRecord is
     * not found then throws an {@link AbandonTaskException}.
     *
     * @param fanoutTask The FanoutTask to handle
     */
    private void handleFanoutTaskOrAbandonTask(final FanoutTask fanoutTask) {
        try {
            backEnd.handleFanoutTask(fanoutTask);
        } catch (NoSuchObjectException e) {
            LOGGER.log(Level.SEVERE, "Pipeline is fatally corrupted. Fanout task record not found", e);
            throw new AbandonTaskException();
        }
    }

    /**
     * @param slot delayed value slot
     */
    public void registerDelayedValue(
            final UpdateSpec spec, final JobRecord generatorJobRecord, final long delaySec, final Slot slot) {
        final UUID pipelineKey = generatorJobRecord.getPipelineKey();
        final QueueSettings queueSettings = generatorJobRecord.getQueueSettings();
        final DelayedSlotFillTask task = new DelayedSlotFillTask(slot, delaySec, pipelineKey, queueSettings);
        spec.getFinalTransaction().registerTask(task);
    }

    /**
     * The root job instance used to wrap a user provided root if the user
     * provided root job has exceptionHandler specified.
     */
    private static final class RootJobInstance extends Job0<Object> {

        private static final long serialVersionUID = -2162670129577469245L;

        private final Job<?> jobInstance;
        private final JobSetting[] settings;
        private final Object[] params;

        RootJobInstance(final Job<?> jobInstance, final JobSetting[] settings, final Object[] params) {
            this.jobInstance = jobInstance;
            this.settings = settings;
            this.params = params;
        }

        @Override
        public Value<Object> run() throws Exception {
            return futureCallUnchecked(settings, jobInstance, params);
        }

        @Override
        public String getJobDisplayName() {
            return jobInstance.getJobDisplayName();
        }
    }

    /**
     * A RuntimeException which, when thrown, causes us to abandon the current
     * task, by returning a 200.
     */
    private class AbandonTaskException extends RuntimeException {
        private final long serialVersionUID = 358437646006972459L;
    }
}
