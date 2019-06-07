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

import com.google.appengine.api.backends.BackendService;
import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.modules.ModulesService;
import com.google.appengine.api.modules.ModulesServiceFactory;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.IntValuedSetting;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import com.google.appengine.tools.pipeline.JobSetting.OnBackend;
import com.google.appengine.tools.pipeline.JobSetting.OnModule;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.appengine.tools.pipeline.JobSetting.WaitForSetting;
import com.google.appengine.tools.pipeline.impl.FutureValueImpl;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.StructReader;
import com.google.common.collect.ImmutableList;

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
public class JobRecord extends PipelineModelObject implements JobInfo {

    public static final String EXCEPTION_HANDLER_METHOD_NAME = "handleException";
    public static final String DATA_STORE_KIND = "Job";
    public static final String STATE_PROPERTY = "state";
    public static final String ROOT_JOB_DISPLAY_NAME = "rootJobDisplayName";
    // Data store entity property names
    private static final String JOB_INSTANCE_PROPERTY = "jobInstance";
    private static final String RUN_BARRIER_PROPERTY = "runBarrier";
    private static final String FINALIZE_BARRIER_PROPERTY = "finalizeBarrier";
    private static final String EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY =
            "exceptionHandlingAncestorKey";
    private static final String EXCEPTION_HANDLER_SPECIFIED_PROPERTY = "hasExceptionHandler";
    private static final String EXCEPTION_HANDLER_JOB_KEY_PROPERTY = "exceptionHandlerJobKey";
    private static final String EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY =
            "exceptionHandlerJobGraphGuid";
    private static final String CALL_EXCEPTION_HANDLER_PROPERTY = "callExceptionHandler";
    private static final String IGNORE_EXCEPTION_PROPERTY = "ignoreException";
    private static final String OUTPUT_SLOT_PROPERTY = "outputSlot";
    private static final String EXCEPTION_KEY_PROPERTY = "exceptionKey";
    private static final String START_TIME_PROPERTY = "startTime";
    private static final String END_TIME_PROPERTY = "endTime";
    private static final String CHILD_KEYS_PROPERTY = "childKeys";
    private static final String ATTEMPT_NUM_PROPERTY = "attemptNum";
    private static final String MAX_ATTEMPTS_PROPERTY = "maxAttempts";
    private static final String BACKOFF_SECONDS_PROPERTY = "backoffSeconds";
    private static final String BACKOFF_FACTOR_PROPERTY = "backoffFactor";
    private static final String ON_BACKEND_PROPERTY = "onBackend";
    private static final String ON_MODULE_PROPERTY = "onModule";
    private static final String ON_QUEUE_PROPERTY = "onQueue";
    private static final String MODULE_VERSION_PROPERTY = "moduleVersion";
    private static final String CHILD_GRAPH_GUID_PROPERTY = "childGraphGuid";
    private static final String STATUS_CONSOLE_URL = "statusConsoleUrl";
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
                    EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY,
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
                    ON_BACKEND_PROPERTY,
                    ON_MODULE_PROPERTY,
                    ON_QUEUE_PROPERTY,
                    MODULE_VERSION_PROPERTY,
                    CHILD_GRAPH_GUID_PROPERTY,
                    STATUS_CONSOLE_URL,
                    ROOT_JOB_DISPLAY_NAME
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
    private String exceptionHandlerJobGraphGuid;
    private boolean callExceptionHandler;
    private boolean ignoreException;
    private UUID exceptionKey;
    private Date startTime;
    private Date endTime;
    private String childGraphGuid;
    private List<UUID> childKeys;
    private long attemptNumber;
    private long maxAttempts = JobSetting.MaxAttempts.DEFAULT;
    private long backoffSeconds = JobSetting.BackoffSeconds.DEFAULT;
    private long backoffFactor = JobSetting.BackoffFactor.DEFAULT;
    private String statusConsoleUrl;
    private String rootJobDisplayName;
    // transient fields
    private Barrier runBarrierInflated;
    private Barrier finalizeBarrierInflated;
    private Slot outputSlotInflated;
    private JobInstanceRecord jobInstanceRecordInflated;
    private Throwable exceptionInflated;

    /**
     * Re-constitutes an instance of this class from a Data Store entity.
     *
     * @param entity
     */
    public JobRecord(StructReader entity) {
        super(DATA_STORE_KIND, entity);
        jobInstanceKey = UUID.fromString(entity.getString(JOB_INSTANCE_PROPERTY));
        finalizeBarrierKey = UUID.fromString(entity.getString(FINALIZE_BARRIER_PROPERTY));
        runBarrierKey = UUID.fromString(entity.getString(RUN_BARRIER_PROPERTY));
        outputSlotKey = UUID.fromString(entity.getString(OUTPUT_SLOT_PROPERTY));
        state = State.valueOf(entity.getString(STATE_PROPERTY));
        exceptionHandlingAncestorKey = entity.isNull(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY)
                ? null
                : UUID.fromString(entity.getString(EXCEPTION_HANDLING_ANCESTOR_KEY_PROPERTY));
        exceptionHandlerSpecified = !entity.isNull(EXCEPTION_HANDLER_SPECIFIED_PROPERTY)
                && entity.getBoolean(EXCEPTION_HANDLER_SPECIFIED_PROPERTY);
        exceptionHandlerJobKey = entity.isNull(EXCEPTION_HANDLER_JOB_KEY_PROPERTY)
                ? null
                : UUID.fromString(entity.getString(EXCEPTION_HANDLER_JOB_KEY_PROPERTY));
        exceptionHandlerJobGraphGuid = entity.isNull(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY)
                ? null
                : entity.getString(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY);
        callExceptionHandler = !entity.isNull(CALL_EXCEPTION_HANDLER_PROPERTY)
                && entity.getBoolean(CALL_EXCEPTION_HANDLER_PROPERTY);
        ignoreException = !entity.isNull(IGNORE_EXCEPTION_PROPERTY)
                && entity.getBoolean(IGNORE_EXCEPTION_PROPERTY);
        childGraphGuid = entity.isNull(CHILD_GRAPH_GUID_PROPERTY)
                ? null
                : entity.getString(CHILD_GRAPH_GUID_PROPERTY);
        exceptionKey = entity.isNull(EXCEPTION_KEY_PROPERTY)
                ? null
                : UUID.fromString(entity.getString(EXCEPTION_KEY_PROPERTY));
        startTime = entity.isNull(START_TIME_PROPERTY)
                ? null
                : entity.getTimestamp(START_TIME_PROPERTY).toDate();
        endTime = entity.isNull(END_TIME_PROPERTY)
                ? null
                : entity.getTimestamp(END_TIME_PROPERTY).toDate();
        childKeys = getUuidListProperty(CHILD_KEYS_PROPERTY, entity)
                .orElse(new LinkedList<>());

        attemptNumber = entity.isNull(ATTEMPT_NUM_PROPERTY)
                ? 0
                : entity.getLong(ATTEMPT_NUM_PROPERTY);
        maxAttempts = entity.getLong(MAX_ATTEMPTS_PROPERTY);
        backoffSeconds = entity.getLong(BACKOFF_SECONDS_PROPERTY);
        backoffFactor = entity.getLong(BACKOFF_FACTOR_PROPERTY);
        queueSettings.setOnBackend(entity.isNull(ON_BACKEND_PROPERTY) ? null : entity.getString(ON_BACKEND_PROPERTY));
        queueSettings.setOnModule(entity.isNull(ON_MODULE_PROPERTY) ? null : entity.getString(ON_MODULE_PROPERTY));
        queueSettings.setModuleVersion(entity.isNull(MODULE_VERSION_PROPERTY) ? null : entity.getString(MODULE_VERSION_PROPERTY));
        queueSettings.setOnQueue(entity.isNull(ON_QUEUE_PROPERTY) ? null : entity.getString(ON_QUEUE_PROPERTY));
        statusConsoleUrl = entity.isNull(STATUS_CONSOLE_URL)
                ? null
                : entity.getString(STATUS_CONSOLE_URL);
        rootJobDisplayName = entity.isNull(ROOT_JOB_DISPLAY_NAME)
                ? null
                : entity.getString(ROOT_JOB_DISPLAY_NAME);
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
     * @param graphGUIDParam       The GUID of the local graph of this job.
     * @param jobInstance          The non-null user-supplied instance of {@code Job} that
     *                             implements the Job that the newly created JobRecord represents.
     * @param callExceptionHandler The flag that indicates that this job should call
     *                             {@code Job#handleException(Throwable)} instead of {@code run}.
     * @param settings             Array of {@code JobSetting} to apply to the newly created
     *                             JobRecord.
     */
    public JobRecord(JobRecord generatorJob, String graphGUIDParam, Job<?> jobInstance,
                     boolean callExceptionHandler, JobSetting[] settings) {
        this(generatorJob.getRootJobKey(), null, generatorJob.getKey(), graphGUIDParam, jobInstance,
                callExceptionHandler, settings, generatorJob.getQueueSettings());
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
        Map<Class<? extends JobSetting>, JobSetting> settingsMap = new HashMap<>();
        for (JobSetting setting : settings) {
            settingsMap.put(setting.getClass(), setting);
        }
        if (!settingsMap.containsKey(StatusConsoleUrl.class)) {
            statusConsoleUrl = generatorJob.statusConsoleUrl;
        }
    }

    private JobRecord(UUID rootJobKey, UUID thisKey, UUID generatorJobKey, String graphGUID,
                      Job<?> jobInstance, boolean callExceptionHandler, JobSetting[] settings,
                      QueueSettings parentQueueSettings) {
        super(DATA_STORE_KIND, rootJobKey, null, thisKey, generatorJobKey, graphGUID);
        jobInstanceRecordInflated = new JobInstanceRecord(this, jobInstance);
        jobInstanceKey = jobInstanceRecordInflated.getKey();
        exceptionHandlerSpecified = isExceptionHandlerSpecified(jobInstance);
        this.callExceptionHandler = callExceptionHandler;
        runBarrierInflated = new Barrier(Barrier.Type.RUN, this);
        runBarrierKey = runBarrierInflated.getKey();
        finalizeBarrierInflated = new Barrier(Barrier.Type.FINALIZE, this);
        finalizeBarrierKey = finalizeBarrierInflated.getKey();
        outputSlotInflated = new Slot(getRootJobKey(), getGeneratorJobKey(), getGraphGuid());
        // Initially we set the filler of the output slot to be this Job.
        // During finalize we may reset it to the filler of the finalize slot.
        outputSlotInflated.setSourceJobKey(getKey());
        outputSlotKey = outputSlotInflated.getKey();
        childKeys = new LinkedList<>();
        state = State.WAITING_TO_RUN;
        for (JobSetting setting : settings) {
            applySetting(setting);
        }
        if (parentQueueSettings != null) {
            queueSettings.merge(parentQueueSettings);
        }
        if (queueSettings.getOnBackend() == null) {
            String module = queueSettings.getOnModule();
            if (module == null) {
                String currentBackend = getCurrentBackend();
                if (currentBackend != null) {
                    queueSettings.setOnBackend(currentBackend);
                } else {
                    ModulesService modulesService = ModulesServiceFactory.getModulesService();
                    queueSettings.setOnModule(modulesService.getCurrentModule());
                    queueSettings.setModuleVersion(modulesService.getCurrentVersion());
                }
            } else {
                ModulesService modulesService = ModulesServiceFactory.getModulesService();
                if (module.equals(modulesService.getCurrentModule())) {
                    queueSettings.setModuleVersion(modulesService.getCurrentVersion());
                } else {
                    queueSettings.setModuleVersion(modulesService.getDefaultVersion(module));
                }
            }
        }
    }

    // Constructor for Root Jobs (called by {@link #createRootJobRecord}).
    private JobRecord(UUID key, Job<?> jobInstance, JobSetting[] settings) {
        // Root Jobs have their rootJobKey the same as their keys and provide null for generatorKey
        // and graphGUID. Also, callExceptionHandler is always false.
        this(key, key, null, null, jobInstance, false, settings, null);
        rootJobDisplayName = jobInstance.getJobDisplayName();
    }

    private static String getCurrentBackend() {
        if (Boolean.parseBoolean(System.getenv("GAE_VM"))) {
            // MVM can't be a backend.
            return null;
        }
        BackendService backendService = BackendServiceFactory.getBackendService();
        String currentBackend = backendService.getCurrentBackend();
        // If currentBackend contains ':' it is actually a B type module (see b/12893879)
        if (currentBackend != null && currentBackend.indexOf(':') != -1) {
            currentBackend = null;
        }
        return currentBackend;
    }

    /**
     * A factory method for root jobs.
     *
     * @param jobInstance The non-null user-supplied instance of {@code Job} that
     *                    implements the Job that the newly created JobRecord represents.
     * @param settings    Array of {@code JobSetting} to apply to the newly created
     *                    JobRecord.
     */
    public static JobRecord createRootJobRecord(Job<?> jobInstance, JobSetting[] settings) {
        UUID key = generateKey(null, DATA_STORE_KIND);
        return new JobRecord(key, jobInstance, settings);
    }

    public static boolean isExceptionHandlerSpecified(Job<?> jobInstance) {
        boolean result = false;
        Class<?> clazz = jobInstance.getClass();
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(EXCEPTION_HANDLER_METHOD_NAME)) {
                Class<?>[] parameterTypes = method.getParameterTypes();
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

    private static boolean checkForInflate(PipelineModelObject obj, UUID expectedGuid, String name) {
        if (null == obj) {
            return false;
        }
        if (!expectedGuid.equals(obj.getKey())) {
            throw new IllegalArgumentException(
                    "Wrong guid for " + name + ". Expected " + expectedGuid + " but was " + obj.getKey());
        }
        return true;
    }

    /**
     * Constructs and returns a Data Store Entity that represents this model
     * object
     *
     * @return
     */
    @Override
    public PipelineMutation toEntity() {
        PipelineMutation mutation = toProtoEntity();
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
        if (null != exceptionHandlerJobGraphGuid) {
            entity.set(EXCEPTION_HANDLER_JOB_GRAPH_GUID_PROPERTY).to(exceptionHandlerJobGraphGuid);
        }
        entity.set(CALL_EXCEPTION_HANDLER_PROPERTY).to(callExceptionHandler);
        entity.set(IGNORE_EXCEPTION_PROPERTY).to(ignoreException);
        if (childGraphGuid != null) {
            entity.set(CHILD_GRAPH_GUID_PROPERTY).to(childGraphGuid);
        }
        entity.set(START_TIME_PROPERTY).to(startTime == null ? null : Timestamp.of(startTime));
        entity.set(END_TIME_PROPERTY).to(endTime == null ? null : Timestamp.of(endTime));
        entity.set(CHILD_KEYS_PROPERTY).toStringArray(childKeys.stream().map(UUID::toString).collect(Collectors.toList()));
        entity.set(ATTEMPT_NUM_PROPERTY).to(attemptNumber);
        entity.set(MAX_ATTEMPTS_PROPERTY).to(maxAttempts);
        entity.set(BACKOFF_SECONDS_PROPERTY).to(backoffSeconds);
        entity.set(BACKOFF_FACTOR_PROPERTY).to(backoffFactor);
        entity.set(ON_BACKEND_PROPERTY).to(queueSettings.getOnBackend());
        entity.set(ON_MODULE_PROPERTY).to(queueSettings.getOnModule());
        entity.set(MODULE_VERSION_PROPERTY).to(queueSettings.getModuleVersion());
        entity.set(ON_QUEUE_PROPERTY).to(queueSettings.getOnQueue());
        entity.set(STATUS_CONSOLE_URL).to(statusConsoleUrl);
        if (rootJobDisplayName != null) {
            entity.set(ROOT_JOB_DISPLAY_NAME).to(rootJobDisplayName);
        }
        return mutation;
    }

    private void applySetting(JobSetting setting) {
        if (setting instanceof WaitForSetting) {
            WaitForSetting wf = (WaitForSetting) setting;
            FutureValueImpl<?> fv = (FutureValueImpl<?>) wf.getValue();
            Slot slot = fv.getSlot();
            runBarrierInflated.addPhantomArgumentSlot(slot);
        } else if (setting instanceof IntValuedSetting) {
            int value = ((IntValuedSetting) setting).getValue();
            if (setting instanceof BackoffSeconds) {
                backoffSeconds = value;
            } else if (setting instanceof BackoffFactor) {
                backoffFactor = value;
            } else if (setting instanceof MaxAttempts) {
                maxAttempts = value;
            } else {
                throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
            }
        } else if (setting instanceof OnBackend) {
            queueSettings.setOnBackend(((OnBackend) setting).getValue());
        } else if (setting instanceof OnModule) {
            queueSettings.setOnModule(((OnModule) setting).getValue());
        } else if (setting instanceof OnQueue) {
            queueSettings.setOnQueue(((OnQueue) setting).getValue());
        } else if (setting instanceof StatusConsoleUrl) {
            statusConsoleUrl = ((StatusConsoleUrl) setting).getValue();
        } else {
            throw new RuntimeException("Unrecognized JobOption class " + setting.getClass().getName());
        }
    }

    @Override
    protected String getDatastoreKind() {
        return DATA_STORE_KIND;
    }

    public void inflate(Barrier runBarrier, Barrier finalizeBarrier, Slot outputSlot,
                        JobInstanceRecord jobInstanceRecord, ExceptionRecord exceptionRecord) {
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
    public void setOutputSlotInflated(Slot outputSlot) {
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

    public void setStartTime(Date date) {
        startTime = date;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date date) {
        endTime = date;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public boolean isExceptionHandlerSpecified() {
        // If this job is exception handler itself then it has exceptionHandlerSpecified
        // but it shouldn't delegate to it.
        return exceptionHandlerSpecified && (!isCallExceptionHandler());
    }

    /**
     * Returns key of the nearest ancestor that has exceptionHandler method
     * overridden or <code>null</code> if none of them has it.
     */
    public UUID getExceptionHandlingAncestorKey() {
        return exceptionHandlingAncestorKey;
    }

    public UUID getExceptionHandlerJobKey() {
        return exceptionHandlerJobKey;
    }

    public String getExceptionHandlerJobGraphGuid() {
        return exceptionHandlerJobGraphGuid;
    }

    public void setExceptionHandlerJobGraphGuid(String exceptionHandlerJobGraphGuid) {
        this.exceptionHandlerJobGraphGuid = exceptionHandlerJobGraphGuid;
    }

    /**
     * If true then this job is exception handler and
     * {@code Job#handleException(Throwable)} should be called instead of <code>run
     * </code>.
     */
    public boolean isCallExceptionHandler() {
        return callExceptionHandler;
    }

    /**
     * If <code>true</code> then an exception during a job execution is ignored. It is
     * expected to be set to <code>true</code> for jobs that execute error handler due
     * to cancellation.
     */
    public boolean isIgnoreException() {
        return ignoreException;
    }

    public void setIgnoreException(boolean ignoreException) {
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

    public void setStatusConsoleUrl(String statusConsoleUrl) {
        this.statusConsoleUrl = statusConsoleUrl;
    }

    public void appendChildKey(UUID key) {
        childKeys.add(key);
    }

    public List<UUID> getChildKeys() {
        return childKeys;
    }

    public String getChildGraphGuid() {
        return childGraphGuid;
    }

    public void setChildGraphGuid(String guid) {
        childGraphGuid = guid;
    }

    @Override
    public JobInfo.State getJobState() {
        switch (state) {
            case WAITING_TO_RUN:
            case WAITING_TO_FINALIZE:
                return JobInfo.State.RUNNING;
            case FINALIZED:
                return JobInfo.State.COMPLETED_SUCCESSFULLY;
            case CANCELED:
                return JobInfo.State.CANCELED_BY_REQUEST;
            case STOPPED:
                if (null == exceptionKey) {
                    return JobInfo.State.STOPPED_BY_REQUEST;
                } else {
                    return JobInfo.State.STOPPED_BY_ERROR;
                }
            case RETRY:
                return JobInfo.State.WAITING_TO_RETRY;
            default:
                throw new RuntimeException("Unrecognized state: " + state);
        }
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

    public void setExceptionKey(UUID exceptionKey) {
        this.exceptionKey = exceptionKey;
    }

    public String getRootJobDisplayName() {
        return rootJobDisplayName;
    }

    private String getJobInstanceString() {
        if (null == jobInstanceRecordInflated) {
            return "jobInstanceKey=" + jobInstanceKey;
        }
        String jobClass = jobInstanceRecordInflated.getClassName();
        return jobClass + (callExceptionHandler ? ".handleException" : ".run");
    }

    @Override
    public String toString() {
        return "JobRecord [" + getKeyName(getKey()) + ", " + state + ", " + getJobInstanceString()
                + ", callExceptionJHandler=" + callExceptionHandler + ", runBarrier="
                + runBarrierKey + ", finalizeBarrier=" + finalizeBarrierKey
                + ", outputSlot=" + outputSlotKey + ", rootJobDisplayName="
                + rootJobDisplayName + ", parent=" + getKeyName(getGeneratorJobKey()) + ", guid="
                + getGraphGuid() + ", childGuid=" + childGraphGuid + "]";
    }

    /**
     * The state of the job.
     */
    public static enum State {
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
    public static enum InflationType {
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
        FOR_OUTPUT;
    }
}
