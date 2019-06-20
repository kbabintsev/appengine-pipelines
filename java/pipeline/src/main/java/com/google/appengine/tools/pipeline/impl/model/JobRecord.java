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

import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.Routing;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.Route;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;
import com.google.appengine.tools.pipeline.impl.util.CallingBackLogger;
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;
import com.google.appengine.tools.pipeline.impl.util.ServiceUtils;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * The Pipeline model object corresponding to a job.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class JobRecord extends PipelineModelObject implements JobInfo {

    public static final String EXCEPTION_HANDLER_METHOD_NAME = "handleException";
    public static final int STATUS_MESSAGES_MAX_COUNT = 1000;
    public static final int STATUS_MESSAGES_MAX_LENGTH = 1000;
    public static final String DATA_STORE_KIND = "Job";
    public static final String STATE_PROPERTY = "state";
    // Data store entity property names
    public static final String JOB_INSTANCE_PROPERTY = "jobInstance";
    public static final String RUN_BARRIER_PROPERTY = "runBarrier";
    public static final String FINALIZE_BARRIER_PROPERTY = "finalizeBarrier";
    public static final String EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY =
            "exceptionHandlingAncestorKey";
    public static final String EXCEPTION_HANDLER_SPECIFIED_PROPERTY = "hasExceptionHandler";
    public static final String EXCEPTION_HANDLER_JOB_KEY_PROPERTY = "exceptionHandlerJobKey";
    public static final String EXCEPTION_HANDLER_JOB_GRAPH_KEY_PROPERTY =
            "exceptionHandlerJobGraphKey";
    public static final String CALL_EXCEPTION_HANDLER_PROPERTY = "callExceptionHandler";
    public static final String IGNORE_EXCEPTION_PROPERTY = "ignoreException";
    public static final String OUTPUT_SLOT_PROPERTY = "outputSlot";
    public static final String EXCEPTION_KEY_PROPERTY = "exceptionKey";
    public static final String START_TIME_PROPERTY = "startTime";
    public static final String END_TIME_PROPERTY = "endTime";
    public static final String CHILD_KEYS_PROPERTY = "childKeys";
    public static final String ATTEMPT_NUM_PROPERTY = "attemptNum";
    public static final String MAX_ATTEMPTS_PROPERTY = "maxAttempts";
    public static final String BACKOFF_SECONDS_PROPERTY = "backoffSeconds";
    public static final String BACKOFF_FACTOR_PROPERTY = "backoffFactor";
    public static final String ON_QUEUE_PROPERTY = "onQueue";
    public static final String ROUTE_PROPERY = "route";
    public static final String CHILD_GRAPH_KEY_PROPERTY = "childGraphKey";
    public static final String STATUS_CONSOLE_URL = "statusConsoleUrl";
    public static final String STATUS_MESSAGES = "statusMessages";
    public static final List<String> PROPERTIES = ImmutableList.<String>builder()
            .addAll(BASE_PROPERTIES)
            .add(
                    JOB_INSTANCE_PROPERTY,
                    RUN_BARRIER_PROPERTY,
                    FINALIZE_BARRIER_PROPERTY,
                    STATE_PROPERTY,
                    EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY,
                    EXCEPTION_HANDLER_SPECIFIED_PROPERTY,
                    EXCEPTION_HANDLER_JOB_KEY_PROPERTY,
                    EXCEPTION_HANDLER_JOB_GRAPH_KEY_PROPERTY,
                    CALL_EXCEPTION_HANDLER_PROPERTY,
                    IGNORE_EXCEPTION_PROPERTY,
                    OUTPUT_SLOT_PROPERTY,
                    EXCEPTION_KEY_PROPERTY,
                    START_TIME_PROPERTY,
                    END_TIME_PROPERTY,
                    CHILD_KEYS_PROPERTY,
                    ATTEMPT_NUM_PROPERTY,
                    MAX_ATTEMPTS_PROPERTY,
                    BACKOFF_SECONDS_PROPERTY,
                    BACKOFF_FACTOR_PROPERTY,
                    ON_QUEUE_PROPERTY,
                    ROUTE_PROPERY,
                    CHILD_GRAPH_KEY_PROPERTY,
                    STATUS_CONSOLE_URL,
                    STATUS_MESSAGES
            )
            .build();
    // persistent fields
    private final UUID jobInstanceKey;
    private final UUID runBarrierKey;
    private final UUID finalizeBarrierKey;
    private final QueueSettings queueSettings = new QueueSettings();
    private UUID outputSlotKey;
    private State state;
    private UUID exceptionHandlingAncestorKey;
    private boolean exceptionHandlerSpecified;
    private UUID exceptionHandlerJobKey;
    private String exceptionHandlerJobGraphKey;
    private boolean callExceptionHandler;
    private boolean ignoreException;
    private UUID exceptionKey;
    private Date startTime;
    private Date endTime;
    private UUID childGraphKey;
    private List<UUID> childKeys;
    private long attemptNumber;
    private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
    private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
    private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;
    private String statusConsoleUrl;
    private List<String> statusMessages;
    // transient fields
    private Barrier runBarrierInflated;
    private Barrier finalizeBarrierInflated;
    private Slot outputSlotInflated;
    private JobInstanceRecord jobInstanceRecordInflated;
    private Throwable exceptionInflated;
    private CallingBackLogger statusLogger = new CallingBackLogger(this::addStatusMessage);

    /**
     * Re-constitutes an instance of this class from a Data Store entity.
     *
     * @param entity structure to read JobRecord from
     */
    public JobRecord(@Nullable final String prefix, @Nonnull final StructReader entity) {
        super(DATA_STORE_KIND, prefix, entity);
        jobInstanceKey = UUID.fromString(entity.getString(Record.property(prefix, JOB_INSTANCE_PROPERTY)));
        finalizeBarrierKey = UUID.fromString(entity.getString(Record.property(prefix, FINALIZE_BARRIER_PROPERTY)));
        runBarrierKey = UUID.fromString(entity.getString(Record.property(prefix, RUN_BARRIER_PROPERTY)));
        outputSlotKey = UUID.fromString(entity.getString(Record.property(prefix, OUTPUT_SLOT_PROPERTY)));
        state = State.valueOf(entity.getString(Record.property(prefix, STATE_PROPERTY)));
        exceptionHandlingAncestorKey = entity.isNull(Record.property(prefix, EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY))
                ? null
                : UUID.fromString(entity.getString(Record.property(prefix, EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY)));
        exceptionHandlerSpecified = !entity.isNull(Record.property(prefix, EXCEPTION_HANDLER_SPECIFIED_PROPERTY))
                && entity.getBoolean(Record.property(prefix, EXCEPTION_HANDLER_SPECIFIED_PROPERTY));
        exceptionHandlerJobKey = entity.isNull(Record.property(prefix, EXCEPTION_HANDLER_JOB_KEY_PROPERTY))
                ? null
                : UUID.fromString(entity.getString(Record.property(prefix, EXCEPTION_HANDLER_JOB_KEY_PROPERTY)));
        exceptionHandlerJobGraphKey = entity.isNull(Record.property(prefix, EXCEPTION_HANDLER_JOB_GRAPH_KEY_PROPERTY))
                ? null
                : entity.getString(Record.property(prefix, EXCEPTION_HANDLER_JOB_GRAPH_KEY_PROPERTY));
        callExceptionHandler = !entity.isNull(Record.property(prefix, CALL_EXCEPTION_HANDLER_PROPERTY))
                && entity.getBoolean(Record.property(prefix, CALL_EXCEPTION_HANDLER_PROPERTY));
        ignoreException = !entity.isNull(Record.property(prefix, IGNORE_EXCEPTION_PROPERTY))
                && entity.getBoolean(Record.property(prefix, IGNORE_EXCEPTION_PROPERTY));
        childGraphKey = entity.isNull(Record.property(prefix, CHILD_GRAPH_KEY_PROPERTY))
                ? null
                : UUID.fromString(entity.getString(Record.property(prefix, CHILD_GRAPH_KEY_PROPERTY)));
        exceptionKey = entity.isNull(Record.property(prefix, EXCEPTION_KEY_PROPERTY))
                ? null
                : UUID.fromString(entity.getString(Record.property(prefix, EXCEPTION_KEY_PROPERTY)));
        startTime = entity.isNull(Record.property(prefix, START_TIME_PROPERTY))
                ? null
                : entity.getTimestamp(Record.property(prefix, START_TIME_PROPERTY)).toDate();
        endTime = entity.isNull(Record.property(prefix, END_TIME_PROPERTY))
                ? null
                : entity.getTimestamp(Record.property(prefix, END_TIME_PROPERTY)).toDate();
        childKeys = getUuidListProperty(Record.property(prefix, CHILD_KEYS_PROPERTY), entity)
                .orElse(new LinkedList<>());

        attemptNumber = entity.isNull(Record.property(prefix, ATTEMPT_NUM_PROPERTY))
                ? 0
                : entity.getLong(Record.property(prefix, ATTEMPT_NUM_PROPERTY));
        maxAttempts = entity.getLong(Record.property(prefix, MAX_ATTEMPTS_PROPERTY));
        backoffSeconds = entity.getLong(Record.property(prefix, BACKOFF_SECONDS_PROPERTY));
        backoffFactor = entity.getLong(Record.property(prefix, BACKOFF_FACTOR_PROPERTY));
        try {
            queueSettings.setRoute(entity.isNull(Record.property(prefix, ROUTE_PROPERY)) ? null : JsonUtils.deserialize(entity.getString(Record.property(prefix, ROUTE_PROPERY)), Route.class));
        } catch (IOException e) {
            throw new RuntimeException("Can't parse '" + ROUTE_PROPERY + "'", e);
        }
        queueSettings.setOnQueue(entity.isNull(Record.property(prefix, ON_QUEUE_PROPERTY)) ? null : entity.getString(Record.property(prefix, ON_QUEUE_PROPERTY)));
        statusConsoleUrl = entity.isNull(Record.property(prefix, STATUS_CONSOLE_URL))
                ? null
                : entity.getString(Record.property(prefix, STATUS_CONSOLE_URL));
        statusMessages = entity.isNull(Record.property(prefix, STATUS_MESSAGES))
                ? Lists.newArrayList()
                : Lists.newArrayList(entity.getStringList(Record.property(prefix, STATUS_MESSAGES)));
    }

    /**
     * Constructs a new JobRecord given the provided data. The constructed
     * instance will be inflated in the sense that
     * {@link #getJobInstanceInflated()}, {@link #getFinalizeBarrierInflated()},
     * {@link #getOutputSlotInflated()} and {@link #getRunBarrierInflated()} will
     * all not return {@code null}. This constructor is used when a new JobRecord
     * is created during the run() method of a parent job. The parent job is also
     * known as the generator job.
     *
     * @param generatorJob         The parent generator job of this job.
     * @param graphKeyParam        The key of the local graph of this job.
     * @param jobInstance          The non-null user-supplied instance of {@code Job} that
     *                             implements the Job that the newly created JobRecord represents.
     * @param callExceptionHandler The flag that indicates that this job should call
     *                             {@code Job#handleException(Throwable)} instead of {@code run}.
     * @param settings             Array of {@code JobSetting} to apply to the newly created
     *                             JobRecord.
     */
    public JobRecord(
            final PipelineManager pipelineManager,
            final JobRecord generatorJob,
            final UUID graphKeyParam,
            final Job<?> jobInstance,
            final UUID jobKey,
            final boolean callExceptionHandler,
            final JobSetting[] settings
    ) {
        this(
                pipelineManager,
                generatorJob.getRootJobKey(),
                jobKey,
                generatorJob.getKey(),
                graphKeyParam,
                jobInstance,
                callExceptionHandler,
                settings,
                generatorJob.getQueueSettings()
        );
        // If generator job has exception handler then it should be called in case
        // of this job throwing to create an exception handling child job.
        // If callExceptionHandler is true then this job is an exception handling
        // child and its exceptions should be handled by its parent's
        // exceptionHandlingAncestor to avoid infinite recursion.
        if (generatorJob.isExceptionHandlerSpecified() && !callExceptionHandler) {
            exceptionHandlingAncestorKey = generatorJob.getKey();
        } else {
            exceptionHandlingAncestorKey = generatorJob.getExceptionHandlingAncestorKey();
        }
        // Inherit settings from generator job
        final Map<Class<? extends JobSetting>, JobSetting> settingsMap = new HashMap<>();
        for (final JobSetting setting : settings) {
            settingsMap.put(setting.getClass(), setting);
        }
        if (!settingsMap.containsKey(StatusConsoleUrl.class)) {
            statusConsoleUrl = generatorJob.statusConsoleUrl;
        }
    }

    private JobRecord(
            final PipelineManager pipelineManager,
            final UUID rootJobKey,
            final UUID thisKey,
            final UUID generatorJobKey,
            final UUID graphKey,
            final Job<?> jobInstance,
            final boolean callExceptionHandler,
            final JobSetting[] settings,
            final QueueSettings parentQueueSettings
    ) {
        super(DATA_STORE_KIND, rootJobKey, thisKey, generatorJobKey, graphKey);
        jobInstanceRecordInflated = new JobInstanceRecord(pipelineManager, this, jobInstance);
        jobInstanceKey = jobInstanceRecordInflated.getKey();
        exceptionHandlerSpecified = hasExceptionHandler(jobInstance);
        this.callExceptionHandler = callExceptionHandler;
        runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
        runBarrierKey = runBarrierInflated.getKey();
        finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
        finalizeBarrierKey = finalizeBarrierInflated.getKey();
        outputSlotInflated = new Slot(pipelineManager, getRootJobKey(), getGeneratorJobKey(), getGraphKey());
        // Initially we set the filler of the output slot to be this Job.
        // During finalize we may reset it to the filler of the finalize slot.
        outputSlotInflated.setSourceJobKey(getKey());
        outputSlotKey = outputSlotInflated.getKey();
        childKeys = new LinkedList<>();
        state = State.WAITING_TO_RUN;
        for (final JobSetting setting : settings) {
            applySetting(setting);
        }
        if (parentQueueSettings != null) {
            queueSettings.merge(parentQueueSettings);
        }
        if (queueSettings.getRoute() == null) {
            queueSettings.setRoute(
                    new Route()
                            .setService(ServiceUtils.getCurrentService())
                            .setVersion(ServiceUtils.getCurrentVersion())
            );
        }
    }

    // Constructor for Root Jobs (called by {@link #createRootJobRecord}).
    public JobRecord(
            final PipelineManager pipelineManager,
            final UUID key,
            final Job<?> jobInstance,
            final JobSetting[] settings
    ) {
        // Root Jobs have their rootJobKey the same as their keys and provide null for generatorKey
        // and graphKey. Also, callExceptionHandler is always false.
        this(pipelineManager, key, key, null, null, jobInstance, false, settings, null);
    }

    public static List<String> propertiesForSelect(@Nullable final String prefix) {
        return Record.propertiesForSelect(DATA_STORE_KIND, PROPERTIES, prefix);
    }

    public static boolean hasExceptionHandler(final Job<?> jobInstance) {
        boolean result = false;
        final Class<?> clazz = jobInstance.getClass();
        for (final Method method : clazz.getMethods()) {
            if (method.getName().equals(EXCEPTION_HANDLER_METHOD_NAME)) {
                final Class<?>[] parameterTypes = method.getParameterTypes();
                if (parameterTypes.length != 1 || !Throwable.class.isAssignableFrom(parameterTypes[0])) {
                    throw new RuntimeException(method
                            + " has invalid signature. It must have exactly one paramter of type "
                            + "Throwable or any of its descendants");
                }
                result = true;
                // continue looping to check signature of all handleException methods
            }
        }
        return result;
    }

    private static boolean checkForInflate(final PipelineModelObject obj, final UUID expectedKey, final String name) {
        if (null == obj) {
            return false;
        }
        if (!expectedKey.equals(obj.getKey())) {
            throw new IllegalArgumentException(
                    "Wrong key for " + name + ". Expected " + expectedKey + " but was " + obj.getKey());
        }
        return true;
    }

    /**
     * Constructs and returns a Data Store Entity that represents this model
     * object
     *
     * @return the mutation for the database
     */
    @Override
    public PipelineMutation toEntity() {
        final PipelineMutation mutation = toProtoEntity();
        final Mutation.WriteBuilder entity = mutation.getDatabaseMutation();
        entity.set(JOB_INSTANCE_PROPERTY).to(jobInstanceKey.toString());
        entity.set(FINALIZE_BARRIER_PROPERTY).to(finalizeBarrierKey.toString());
        entity.set(RUN_BARRIER_PROPERTY).to(runBarrierKey.toString());
        entity.set(OUTPUT_SLOT_PROPERTY).to(outputSlotKey.toString());
        entity.set(STATE_PROPERTY).to(state.toString());
        if (null != exceptionHandlingAncestorKey) {
            entity.set(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY).to(exceptionHandlingAncestorKey.toString());
        }
        if (exceptionHandlerSpecified) {
            entity.set(EXCEPTION_HANDLER_SPECIFIED_PROPERTY).to(true);
        }
        if (null != exceptionHandlerJobKey) {
            entity.set(EXCEPTION_HANDLER_JOB_KEY_PROPERTY).to(exceptionHandlerJobKey.toString());
        }
        if (null != exceptionKey) {
            entity.set(EXCEPTION_KEY_PROPERTY).to(exceptionKey.toString());
        }
        if (null != exceptionHandlerJobGraphKey) {
            entity.set(EXCEPTION_HANDLER_JOB_GRAPH_KEY_PROPERTY).to(exceptionHandlerJobGraphKey);
        }
        entity.set(CALL_EXCEPTION_HANDLER_PROPERTY).to(callExceptionHandler);
        entity.set(IGNORE_EXCEPTION_PROPERTY).to(ignoreException);
        if (childGraphKey != null) {
            entity.set(CHILD_GRAPH_KEY_PROPERTY).to(childGraphKey.toString());
        }
        entity.set(START_TIME_PROPERTY).to(startTime == null ? null : Timestamp.of(startTime));
        entity.set(END_TIME_PROPERTY).to(endTime == null ? null : Timestamp.of(endTime));
        entity.set(CHILD_KEYS_PROPERTY).toStringArray(childKeys.stream().map(UUID::toString).collect(Collectors.toList()));
        entity.set(ATTEMPT_NUM_PROPERTY).to(attemptNumber);
        entity.set(MAX_ATTEMPTS_PROPERTY).to(maxAttempts);
        entity.set(BACKOFF_SECONDS_PROPERTY).to(backoffSeconds);
        entity.set(BACKOFF_FACTOR_PROPERTY).to(backoffFactor);
        try {
            entity.set(ROUTE_PROPERY).to(queueSettings.getRoute() == null ? null : JsonUtils.serialize(queueSettings.getRoute()));
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize '" + ROUTE_PROPERY + "'");
        }
        entity.set(ON_QUEUE_PROPERTY).to(queueSettings.getOnQueue());
        entity.set(STATUS_CONSOLE_URL).to(statusConsoleUrl);
        List<String> messages = statusMessages;
        if (messages != null && messages.size() > STATUS_MESSAGES_MAX_COUNT) {
            messages = messages.subList(messages.size() - STATUS_MESSAGES_MAX_COUNT, messages.size());
            messages.set(0, "[Truncated]");
        }
        entity.set(STATUS_MESSAGES).toStringArray(messages);
        return mutation;
    }

    private void applySetting(final JobSetting setting) {
        if (setting instanceof WaitForSetting) {
            final WaitForSetting wf = (WaitForSetting) setting;
            final FutureValueImpl<?> fv = (FutureValueImpl<?>) wf.getValue();
            final Slot slot = fv.getSlot();
            runBarrierInflated.addPhantomArgumentSlot(slot);
        } else if (setting instanceof IntValuedSetting) {
            final int value = ((IntValuedSetting) setting).getValue();
            if (setting instanceof BackoffSeconds) {
                backoffSeconds = value;
            } else if (setting instanceof BackoffFactor) {
                backoffFactor = value;
            } else if (setting instanceof MaxAttempts) {
                maxAttempts = value;
            } else {
                throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
            }
        } else if (setting instanceof OnQueue) {
            queueSettings.setOnQueue(((OnQueue) setting).getValue());
        } else if (setting instanceof Routing) {
            queueSettings.setRoute(((Routing) setting).getRoute());
        } else if (setting instanceof StatusConsoleUrl) {
            statusConsoleUrl = ((StatusConsoleUrl) setting).getValue();
        } else {
            throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
        }
    }

    @Override
    public String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    public void inflate(final Barrier runBarrier, final Barrier finalizeBarrier, final Slot outputSlot,
                        final JobInstanceRecord jobInstanceRecord, final ExceptionRecord exceptionRecord) {
        if (checkForInflate(runBarrier, runBarrierKey, "runBarrier")) {
            runBarrierInflated = runBarrier;
        }
        if (checkForInflate(finalizeBarrier, finalizeBarrierKey, "finalizeBarrier")) {
            finalizeBarrierInflated = finalizeBarrier;
        }
        if (checkForInflate(outputSlot, outputSlotKey, "outputSlot")) {
            outputSlotInflated = outputSlot;
        }
        if (checkForInflate(jobInstanceRecord, jobInstanceKey, "jobInstanceRecord")) {
            jobInstanceRecordInflated = jobInstanceRecord;
        }
        if (checkForInflate(exceptionRecord, exceptionKey, "exception")) {
            exceptionInflated = exceptionRecord.getException();
        }
    }

    public UUID getRunBarrierKey() {
        return runBarrierKey;
    }

    public Barrier getRunBarrierInflated() {
        return runBarrierInflated;
    }

    public UUID getFinalizeBarrierKey() {
        return finalizeBarrierKey;
    }

    public Barrier getFinalizeBarrierInflated() {
        return finalizeBarrierInflated;
    }

    public UUID getOutputSlotKey() {
        return outputSlotKey;
    }

    public Slot getOutputSlotInflated() {
        return outputSlotInflated;
    }

    /**
     * Used to set exceptionHandling Job output to the same slot as the protected job.
     */
    public void setOutputSlotInflated(final Slot outputSlot) {
        outputSlotInflated = outputSlot;
        outputSlotKey = outputSlot.getKey();
    }

    public UUID getJobInstanceKey() {
        return jobInstanceKey;
    }

    public JobInstanceRecord getJobInstanceInflated() {
        return jobInstanceRecordInflated;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(final Date date) {
        startTime = date;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(final Date date) {
        endTime = date;
    }

    public State getState() {
        return state;
    }

    public void setState(final State state) {
        this.state = state;
    }

    public boolean isExceptionHandlerSpecified() {
        // If this job is exception handler itself then it has exceptionHandlerSpecified
        // but it shouldn't delegate to it.
        return exceptionHandlerSpecified && (!isCallExceptionHandler());
    }

    /**
     * Returns key of the nearest ancestor that has exceptionHandler method
     * overridden or {@code null} if none of them has it.
     */
    public UUID getExceptionHandlingAncestorKey() {
        return exceptionHandlingAncestorKey;
    }

    public UUID getExceptionHandlerJobKey() {
        return exceptionHandlerJobKey;
    }

    public String getExceptionHandlerJobGraphKey() {
        return exceptionHandlerJobGraphKey;
    }

    public void setExceptionHandlerJobGraphKey(final String exceptionHandlerJobGraphKey) {
        this.exceptionHandlerJobGraphKey = exceptionHandlerJobGraphKey;
    }

    /**
     * If true then this job is exception handler and
     * {@code Job#handleException(Throwable)} should be called instead of {@code run
     * }.
     */
    public boolean isCallExceptionHandler() {
        return callExceptionHandler;
    }

    /**
     * If {@code true} then an exception during a job execution is ignored. It is
     * expected to be set to {@code true} for jobs that execute error handler due
     * to cancellation.
     */
    public boolean isIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(final boolean ignoreException) {
        this.ignoreException = ignoreException;
    }

    public int getAttemptNumber() {
        return (int) attemptNumber;
    }

    public void incrementAttemptNumber() {
        attemptNumber++;
    }

    public int getBackoffSeconds() {
        return (int) backoffSeconds;
    }

    public int getBackoffFactor() {
        return (int) backoffFactor;
    }

    public int getMaxAttempts() {
        return (int) maxAttempts;
    }

    /**
     * Returns a copy of QueueSettings
     */
    public QueueSettings getQueueSettings() {
        return queueSettings;
    }

    public String getStatusConsoleUrl() {
        return statusConsoleUrl;
    }

    public void setStatusConsoleUrl(final String statusConsoleUrl) {
        this.statusConsoleUrl = statusConsoleUrl;
    }

    public void addStatusMessage(final String text) {
        if (statusMessages == null) {
            statusMessages = Lists.newArrayList();
        }
        final String line = "[" + attemptNumber + "] " + DateTime.now().toString() + ": " + text;
        statusMessages.add(line.length() > STATUS_MESSAGES_MAX_LENGTH ? line.substring(0, STATUS_MESSAGES_MAX_LENGTH - 3) + "..." : line);
    }

    public Logger getStatusLogger() {
        return statusLogger;
    }

    public List<String> getStatusMessages() {
        return statusMessages;
    }

    public void appendChildKey(final UUID key) {
        childKeys.add(key);
    }

    public List<UUID> getChildKeys() {
        return childKeys;
    }

    public UUID getChildGraphKey() {
        return childGraphKey;
    }

    public void setChildGraphKey(final UUID key) {
        childGraphKey = key;
    }

    @Override
    public JobInfo.State getJobState() {
        return JobInfo.convertState(state, exceptionKey);
    }

    @Override
    public Object getOutput() {
        if (null == outputSlotInflated) {
            return null;
        } else {
            return outputSlotInflated.getValue();
        }
    }

    @Override
    public String getError() {
        if (exceptionInflated == null) {
            return null;
        }
        return StringUtils.printStackTraceToString(exceptionInflated);
    }

    @Override
    public Throwable getException() {
        return exceptionInflated;
    }

    public UUID getExceptionKey() {
        return exceptionKey;
    }

    public void setExceptionKey(final UUID exceptionKey) {
        this.exceptionKey = exceptionKey;
    }

    private String getJobInstanceString() {
        if (null == jobInstanceRecordInflated) {
            return "jobInstanceKey=" + jobInstanceKey;
        }
        final String jobClass = jobInstanceRecordInflated.getClassName();
        return jobClass + (callExceptionHandler ? ".handleException" : ".run");
    }

    @Override
    public String toString() {
        return "JobRecord [" + getKeyName(getKey()) + ", " + state + ", " + getJobInstanceString()
                + ", callExceptionJHandler=" + callExceptionHandler + ", runBarrier="
                + runBarrierKey + ", finalizeBarrier=" + finalizeBarrierKey
                + ", outputSlot=" + outputSlotKey + ", parent=" + getKeyName(getGeneratorJobKey()) + ", graphKey="
                + getGraphKey() + ", childGraphKey=" + childGraphKey + "]";
    }

    /**
     * The state of the job.
     */
    public enum State {
        // TODO(user): document states (including valid transitions) and relation to JobInfo.State
        WAITING_TO_RUN, WAITING_TO_FINALIZE, FINALIZED, STOPPED, CANCELED, RETRY
    }

    /**
     * This enum serves as an input parameter to the method
     * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#queryJob(
     *UUID, InflationType)}. When fetching an
     * instance of {@code JobRecord} from the data store this enum specifies how
     * much auxiliary data should also be queried and used to inflate the instance
     * of {@code JobRecord}.
     */
    public enum InflationType {
        /**
         * Do not inflate at all
         */
        NONE,

        /**
         * Inflate as necessary to run the job. In particular:
         * <ul>
         * <li>{@link JobRecord#getRunBarrierInflated()} will not return
         * {@code null}; and
         * <li>for the returned {@link Barrier}
         * {@link Barrier#getWaitingOnInflated()} will not return {@code null}; and
         * <li> {@link JobRecord#getOutputSlotInflated()} will not return
         * {@code null}; and
         * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
         * {@code null}
         * </ul>
         */
        FOR_RUN,

        /**
         * Inflate as necessary to finalize the job. In particular:
         * <ul>
         * <li> {@link JobRecord#getOutputSlotInflated()} will not return
         * {@code null}; and
         * <li> {@link JobRecord#getFinalizeBarrierInflated()} will not return
         * {@code null}; and
         * <li>for the returned {@link Barrier} the method
         * {@link Barrier#getWaitingOnInflated()} will not return {@code null}.
         * </ul>
         */
        FOR_FINALIZE,

        /**
         * Inflate as necessary to retrieve the output of the job. In particular
         * {@link JobRecord#getOutputSlotInflated()} will not return {@code null}
         */
        FOR_OUTPUT,
    }
}
