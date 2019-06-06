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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.pipeline.Consts;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.model.Barrier;
import com.google.appengine.tools.pipeline.impl.model.ExceptionRecord;
import com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord;
import com.google.appengine.tools.pipeline.impl.model.JobInstanceRecord;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineModelObject;
import com.google.appengine.tools.pipeline.impl.model.PipelineMutation;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.GUIDGenerator;
import com.google.appengine.tools.pipeline.impl.util.SerializationUtils;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.google.appengine.tools.pipeline.impl.model.JobRecord.ROOT_JOB_DISPLAY_NAME;
import static com.google.appengine.tools.pipeline.impl.model.JobRecord.STATE_PROPERTY;
import static com.google.appengine.tools.pipeline.impl.model.PipelineModelObject.ROOT_JOB_KEY_PROPERTY;
import static com.google.appengine.tools.pipeline.impl.util.TestUtils.throwHereForTesting;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class AppEngineBackEnd implements PipelineBackEnd {

    private static final RetryParams RETRY_PARAMS = new RetryParams.Builder()
            .retryDelayBackoffFactor(2)
            .initialRetryDelayMillis(300)
            .maxRetryDelayMillis(5000)
            .retryMinAttempts(5)
            .retryMaxAttempts(5)
            .build();

    private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
            ConcurrentModificationException.class, DatastoreTimeoutException.class,
            DatastoreFailureException.class)
            .abortOn(EntityNotFoundException.class, NoSuchObjectException.class).build();

    private static final Logger logger = Logger.getLogger(AppEngineBackEnd.class.getName());

    private final Spanner spanner;
    private final DatabaseClient databaseClient;
    private final AppEngineTaskQueue taskQueue;
    private final Storage storage;

    public AppEngineBackEnd() {
        spanner = SpannerOptions.newBuilder().build().getService();
        final DatabaseId logsId = DatabaseId.of(Consts.SPANNER_PROJECT, Consts.SPANNER_INSTANCE, Consts.SPANNER_DATABASE);
        databaseClient = spanner.getDatabaseClient(logsId);
        taskQueue = new AppEngineTaskQueue();
        try {
            storage = new Storage.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    JacksonFactory.getDefaultInstance(),
                    GoogleCredential.getApplicationDefault()
            ).setApplicationName("Cloudaware").build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException("Can't create Storage client", e);
        }
    }

    @Override
    public void shutdown() {
        spanner.close();
    }

    private void addAll(Collection<? extends PipelineModelObject> objects, MutationsAndBlobs allMutations) {
        if (objects.isEmpty()) {
            return;
        }
        for (PipelineModelObject x : objects) {
            logger.finest("Storing: " + x);
            final PipelineMutation m = x.toEntity();
            allMutations.addMutation(m.getDatabaseMutation().build());
            if (m.getBlobMutation() != null) {
                allMutations.addBlob(m.getBlobMutation());
            }
        }
    }

    private void addAll(UpdateSpec.Group group, MutationsAndBlobs allMutations) {
        addAll(group.getBarriers(), allMutations);
        addAll(group.getJobs(), allMutations);
        addAll(group.getSlots(), allMutations);
        addAll(group.getJobInstanceRecords(), allMutations);
        addAll(group.getFailureRecords(), allMutations);
    }

    private void saveAll(UpdateSpec.Group transactionSpec, TransactionContext transaction) {
        MutationsAndBlobs allMutations = new MutationsAndBlobs();
        addAll(transactionSpec, allMutations);
        for (PipelineMutation.BlobMutation blob : allMutations.getBlobs()) {
            saveBlob(blob.getRootJobKey(), blob.getKey(), blob.getValue());
        }
        transaction.buffer(allMutations.getMutations());
    }

    private boolean transactionallySaveAll(UpdateSpec.Transaction transactionSpec,
                                           QueueSettings queueSettings, UUID rootJobKey, UUID jobKey, JobRecord.State... expectedStates) {
        final Boolean out = databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Boolean>() {
            @Nullable
            @Override
            public Boolean run(final TransactionContext transaction) throws Exception {
                if (jobKey != null && expectedStates != null) {
                    Struct entity = transaction.readRow(JobRecord.DATA_STORE_KIND, Key.of(jobKey.toString()), ImmutableList.of(STATE_PROPERTY));
                    if (entity == null) {
                        throw new RuntimeException(
                                "Fatal Pipeline corruption error. No JobRecord found with key = " + jobKey);
                    }
                    JobRecord.State state = JobRecord.State.valueOf(entity.getString(STATE_PROPERTY));
                    boolean stateIsExpected = false;
                    for (JobRecord.State expectedState : expectedStates) {
                        if (state == expectedState) {
                            stateIsExpected = true;
                            break;
                        }
                    }
                    if (!stateIsExpected) {
                        logger.info("Job " + jobKey + " is not in one of the expected states: "
                                + Arrays.asList(expectedStates)
                                + " and so transactionallySaveAll() will not continue.");
                        return false;
                    }
                }
                saveAll(transactionSpec, transaction);
                return true;
            }
        });
        if (transactionSpec instanceof UpdateSpec.TransactionWithTasks) {
            UpdateSpec.TransactionWithTasks transactionWithTasks =
                    (UpdateSpec.TransactionWithTasks) transactionSpec;
            Collection<Task> tasks = transactionWithTasks.getTasks();
            if (tasks.size() > 0) {
                byte[] encodedTasks = FanoutTask.encodeTasks(tasks);
                FanoutTaskRecord ftRecord = new FanoutTaskRecord(rootJobKey, encodedTasks);
                // Store FanoutTaskRecord outside of any transaction, but before
                // the FanoutTask is enqueued. If the put succeeds but the
                // enqueue fails then the FanoutTaskRecord is orphaned. But
                // the Pipeline is still consistent.
                databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(final TransactionContext transaction2) throws Exception {
                        transaction2.buffer(ImmutableList.of(ftRecord.toEntity().getDatabaseMutation().build()));
                        return null;
                    }
                });
                FanoutTask fannoutTask = new FanoutTask(ftRecord.getKey(), queueSettings);
                taskQueue.enqueue(fannoutTask);
            }
        }
        return out == null ? false : out;
    }

//    private <R> R tryFiveTimes(final Operation<R> operation) {
//        try {
//            return RetryHelper.runWithRetries(operation, RETRY_PARAMS, EXCEPTION_HANDLER);
//        } catch (RetriesExhaustedException | NonRetriableException e) {
//            if (e.getCause() instanceof RuntimeException) {
//                logger.info(e.getCause().getMessage() + " during " + operation.getName()
//                        + " throwing after multiple attempts ");
//                throw (RuntimeException) e.getCause();
//            } else {
//                throw e;
//            }
//        }
//    }

    @Override
    public void cleanBlobs(String prefix) {
        try {
            while (true) {
                final Objects objects = storage.objects().list(Consts.BLOB_BUCKET_NAME).setPrefix(prefix).execute();
                if (objects == null || objects.getItems() == null || objects.getItems().isEmpty()) {
                    break;
                }
                logger.info("Deleting " + objects.getItems().size() + " blobs");
                for (StorageObject object : objects.getItems()) {
                    storage.objects().delete(object.getBucket(), object.getName()).execute();
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Can't clean blobs in Storage bucket", ex);
        }
    }

    @Override
    public void saveBlob(UUID rootJobKey, UUID ownerKey, byte[] value) {
        try {
            storage.objects().insert(
                    Consts.BLOB_BUCKET_NAME,
                    new StorageObject()
                            .setBucket(Consts.BLOB_BUCKET_NAME)
                            .setName(rootJobKey + "/" + ownerKey + ".dat"),
                    new ByteArrayContent("application/octet-stream", value)
            ).execute();
        } catch (IOException ex) {
            throw new RuntimeException("Can't save blob in Storage bucket", ex);
        }
    }

    @Override
    public byte[] retrieveBlob(UUID rootJobKey, UUID ownerKey) {
        try {
            final InputStream inputStream = storage.objects().get(
                    Consts.BLOB_BUCKET_NAME,
                    rootJobKey + "/" + ownerKey + ".dat"
            ).executeMediaAsInputStream();
            return ByteStreams.toByteArray(inputStream);
        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() == 404) {
                return null;
            } else {
                throw new RuntimeException("Can't retrieve blob from Storage bucket", ex);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Can't retrieve blob from Storage bucket", ex);
        }
    }

    @Override
    public void enqueue(Task task) {
        taskQueue.enqueue(task);
    }

    @Override
    public boolean saveWithJobStateCheck(final UpdateSpec updateSpec,
                                         final QueueSettings queueSettings, final UUID jobKey,
                                         final JobRecord.State... expectedStates) {
        // tryFiveTimes was here
        databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
            @Nullable
            @Override
            public Void run(final TransactionContext transaction) throws Exception {
                saveAll(updateSpec.getNonTransactionalGroup(), transaction);
                return null;
            }
        });
        for (final UpdateSpec.Transaction transactionSpec : updateSpec.getTransactions()) {
            //tryFiveTimes was here
            transactionallySaveAll(transactionSpec, queueSettings, updateSpec.getRootJobKey(), null);
        }

        // TODO(user): Replace this with plug-able hooks that could be used by tests,
        // if needed could be restricted to package-scoped tests.
        // If a unit test requests us to do so, fail here.
        throwHereForTesting("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
        //tryFiveTimes was here
        boolean wasSaved = transactionallySaveAll(
                updateSpec.getFinalTransaction(), queueSettings, updateSpec.getRootJobKey(), jobKey, expectedStates);
        return wasSaved;
    }

    @Override
    public void save(UpdateSpec updateSpec, QueueSettings queueSettings) {
        saveWithJobStateCheck(updateSpec, queueSettings, null);
    }

    @Override
    public JobRecord queryJob(final UUID jobKey, final JobRecord.InflationType inflationType)
            throws NoSuchObjectException {
        Struct entity = getEntity("queryJob", JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, jobKey);
        JobRecord jobRecord = new JobRecord(entity);
        Barrier runBarrier = null;
        Barrier finalizeBarrier = null;
        Slot outputSlot = null;
        JobInstanceRecord jobInstanceRecord = null;
        ExceptionRecord failureRecord = null;
        switch (inflationType) {
            case FOR_RUN:
                runBarrier = queryBarrier(jobRecord.getRunBarrierKey(), true);
                finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), false);
                jobInstanceRecord =
                        new JobInstanceRecord(getEntity("queryJob", JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, jobRecord.getJobInstanceKey()));
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_FINALIZE:
                finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true);
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_OUTPUT:
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                UUID failureKey = jobRecord.getExceptionKey();
                failureRecord = queryFailure(failureKey);
                break;
            default:
        }
        jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
        logger.finest("Query returned: " + jobRecord);
        return jobRecord;
    }

    /**
     * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
     * will not return {@code null}.
     */
    private Barrier queryBarrier(UUID barrierKey, boolean inflate) throws NoSuchObjectException {
        Struct entity = getEntity("queryBarrier", Barrier.DATA_STORE_KIND, Barrier.PROPERTIES, barrierKey);
        Barrier barrier = new Barrier(entity);
        if (inflate) {
            Collection<Barrier> barriers = new ArrayList<>(1);
            barriers.add(barrier);
            inflateBarriers(barriers);
        }
        logger.finest("Querying returned: " + barrier);
        return barrier;
    }

    /**
     * Given a {@link Collection} of {@link Barrier Barriers}, inflate each of the
     * {@link Barrier Barriers} so that {@link Barrier#getWaitingOnInflated()}
     * will not return null;
     *
     * @param barriers
     */
    private void inflateBarriers(Collection<Barrier> barriers) {
        // Step 1. Build the set of keys corresponding to the slots.
        Set<UUID> keySet = new HashSet<>(barriers.size() * 5);
        for (Barrier barrier : barriers) {
            keySet.addAll(barrier.getWaitingOnKeys());
        }
        // Step 2. Query the datastore for the Slot entities
        // Step 3. Convert into map from key to Slot
        Map<UUID, Slot> slotMap = new HashMap<>(keySet.size());
        getEntities(
                "inflateBarriers",
                Slot.DATA_STORE_KIND,
                Slot.PROPERTIES,
                keySet,
                struct -> {
                    slotMap.put(UUID.fromString(struct.getString(Slot.ID_PROPERTY)), new Slot(struct));
                }
        );

        // Step 4. Inflate each of the barriers
        for (Barrier barrier : barriers) {
            barrier.inflate(slotMap);
        }
    }

    @Override
    public Slot querySlot(UUID slotKey, boolean inflate) throws NoSuchObjectException {
        Struct entity = getEntity("querySlot", Slot.DATA_STORE_KIND, Slot.PROPERTIES, slotKey);
        Slot slot = new Slot(entity);
        if (inflate) {
            Map<UUID, Barrier> barriers = new HashMap<>();
            getEntities(
                    "querySlot",
                    Barrier.DATA_STORE_KIND,
                    Barrier.PROPERTIES,
                    slot.getWaitingOnMeKeys(),
                    struct -> {
                        barriers.put(UUID.fromString(struct.getString(Barrier.ID_PROPERTY)), new Barrier(struct));
                    }
            );
            slot.inflate(barriers);
            inflateBarriers(barriers.values());
        }
        return slot;
    }

    @Override
    public ExceptionRecord queryFailure(UUID failureKey) throws NoSuchObjectException {
        if (failureKey == null) {
            return null;
        }
        Struct entity = getEntity("ReadExceptionRecord", ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, failureKey);
        return new ExceptionRecord(entity);
    }

    @Override
    public byte[] serializeValue(PipelineModelObject model, Object value) throws IOException {
        byte[] bytes = SerializationUtils.serialize(value);
        return bytes;
    }

    @Override
    public Object deserializeValue(PipelineModelObject model, byte[] serializedVersion)
            throws IOException {
        return SerializationUtils.deserialize(serializedVersion);
    }

    private void getEntities(String logString, String tableName, List<String> columns, final Collection<UUID> keys, Consumer<StructReader> consumer) {
        //tryFiveTimes was here
        final KeySet.Builder keysBuilder = KeySet.newBuilder();
        keys.forEach(key -> keysBuilder.addKey(Key.of(key.toString())));
        int count = 0;
        try (ResultSet rs = databaseClient.singleUse().read(
                tableName,
                keysBuilder.build(),
                columns
        )) {
            while (rs.next()) {
                consumer.accept(rs);
                count += 1;
            }
        }

        if (keys.size() != count) {
            throw new RuntimeException("Missing " + (keys.size() - count) + "entities");
        }
    }

    private Struct getEntity(String logString, String tableName, List<String> columns, final UUID key) throws NoSuchObjectException {
        // tryFiveTimes was here
        final Struct entity = databaseClient.singleUse().readRow(tableName, Key.of(key.toString()), columns);
        if (entity == null) {
            throw new NoSuchObjectException(key.toString());
        }
        return entity;
    }

    @Override
    public void handleFanoutTask(FanoutTask fanoutTask) throws NoSuchObjectException {
        UUID fanoutTaskRecordKey = fanoutTask.getRecordKey();
        // Fetch the fanoutTaskRecord outside of any transaction
        Struct entity = getEntity("handleFanoutTask", FanoutTaskRecord.DATA_STORE_KIND, FanoutTaskRecord.PROPERTIES, fanoutTaskRecordKey);
        FanoutTaskRecord ftRecord = new FanoutTaskRecord(entity);
        byte[] encodedBytes = ftRecord.getPayload();
        taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
    }

    public void queryAll(final String tableName, List<String> columns, final UUID rootJobKey, Consumer<StructReader> consumer) {
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                Statement.newBuilder(
                        "SELECT " + String.join(", ", columns) + " "
                                + "FROM " + tableName + " "
                                + "WHERE " + ROOT_JOB_KEY_PROPERTY + " = @rootJobKey"
                ).bind("rootJobKey").to(rootJobKey.toString()).build()
        )) {
            {
                while (rs.next()) {
                    consumer.accept(rs);
                }
            }
        }
    }

    @Override
    public Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(String classFilter,
                                                                          String cursor, final int limit) {
        final Statement.Builder builder = Statement.newBuilder(
                "SELECT DISTINCT " + String.join(", ", JobRecord.PROPERTIES) + " "
                        + "FROM " + JobRecord.DATA_STORE_KIND + " "
                        + "WHERE "
        );
        if (classFilter == null) {
            builder.append(ROOT_JOB_KEY_PROPERTY + " != null ");
        } else {
            builder.append(ROOT_JOB_DISPLAY_NAME + " = @classFilter ")
                    .bind("classFilter").to(classFilter);
        }
        builder.append("ORDER BY " + ROOT_JOB_DISPLAY_NAME + " ");
        // limit not set
        // cursor not used
        List<JobRecord> roots = new LinkedList<>();
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                builder.build()
        )) {
            {
                while (rs.next()) {
                    roots.add(new JobRecord(rs));
                }
            }
        }
        return Pair.of(roots, null);
    }

    @Override
    public Set<String> getRootPipelinesDisplayName() {
        Set<String> pipelines = new LinkedHashSet<>();
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                Statement.newBuilder(
                        "SELECT DISTINCT " + JobRecord.ROOT_JOB_DISPLAY_NAME + " "
                                + "FROM " + JobRecord.DATA_STORE_KIND + " "
                                + "ORDER BY " + JobRecord.ROOT_JOB_DISPLAY_NAME
                ).build()
        )) {
            {
                while (rs.next()) {
                    pipelines.add(rs.getString(JobRecord.ROOT_JOB_DISPLAY_NAME));
                }
            }
        }
        return pipelines;
    }

    @Override
    public Set<UUID> getTestPipelines() {
        Set<UUID> pipelines = new LinkedHashSet<>();
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                Statement.newBuilder(
                        "SELECT DISTINCT " + JobRecord.ROOT_JOB_KEY_PROPERTY + " "
                                + "FROM " + JobRecord.DATA_STORE_KIND + " "
                                + "WHERE " + JobRecord.ROOT_JOB_KEY_PROPERTY + " LIKE '" + GUIDGenerator.TEST_PREFIX + "%'"
                ).build()
        )) {
            {
                while (rs.next()) {
                    pipelines.add(UUID.fromString(rs.getString(JobRecord.ROOT_JOB_KEY_PROPERTY)));
                }
            }
        }
        return pipelines;
    }

    @Override
    public PipelineObjects queryFullPipeline(final UUID rootJobKey) {
        final Map<UUID, JobRecord> jobs = new HashMap<>();
        final Map<UUID, Slot> slots = new HashMap<>();
        final Map<UUID, Barrier> barriers = new HashMap<>();
        final Map<UUID, JobInstanceRecord> jobInstanceRecords = new HashMap<>();
        final Map<UUID, ExceptionRecord> failureRecords = new HashMap<>();

        queryAll(Barrier.DATA_STORE_KIND, Barrier.PROPERTIES, rootJobKey, (struct) -> {
            barriers.put(UUID.fromString(struct.getString(Barrier.ID_PROPERTY)), new Barrier(struct));
        });
        queryAll(Slot.DATA_STORE_KIND, Slot.PROPERTIES, rootJobKey, (struct) -> {
            slots.put(UUID.fromString(struct.getString(Slot.ID_PROPERTY)), new Slot(struct, true));
        });
        queryAll(JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, rootJobKey, (struct) -> {
            jobs.put(UUID.fromString(struct.getString(JobRecord.ID_PROPERTY)), new JobRecord(struct));
        });
        queryAll(JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, rootJobKey, (struct) -> {
            jobInstanceRecords.put(UUID.fromString(struct.getString(JobInstanceRecord.ID_PROPERTY)), new JobInstanceRecord(struct));
        });
        queryAll(ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, rootJobKey, (struct) -> {
            failureRecords.put(UUID.fromString(struct.getString(ExceptionRecord.ID_PROPERTY)), new ExceptionRecord(struct));
        });
        return new PipelineObjects(
                rootJobKey, jobs, slots, barriers, jobInstanceRecords, failureRecords);
    }

    private void deleteAll(final String tableName, final UUID rootJobKey) {
        logger.info("Deleting all " + tableName + " with rootJobKey=" + rootJobKey);
        databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
            @Nullable
            @Override
            public Void run(final TransactionContext transaction) throws Exception {
                final long deleted = transaction.executeUpdate(
                        Statement.newBuilder("DELETE FROM " + tableName + " WHERE " + ROOT_JOB_KEY_PROPERTY + " = @rootJobKey")
                                .bind("rootJobKey").to(rootJobKey.toString())
                                .build()
                );
                return null;
            }
        });
    }

    /**
     * Delete all datastore entities corresponding to the given pipeline.
     *
     * @param rootJobKey The root job key identifying the pipeline
     * @param force      If this parameter is not {@code true} then this method will
     *                   throw an {@link IllegalStateException} if the specified pipeline is not in the
     *                   {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
     *                   {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
     * @param async      If this parameter is {@code true} then instead of performing
     *                   the delete operation synchronously, this method will enqueue a task
     *                   to perform the operation.
     * @throws IllegalStateException If {@code force = false} and the specified
     *                               pipeline is not in the
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
     */
    @Override
    public void deletePipeline(UUID rootJobKey, boolean force, boolean async)
            throws IllegalStateException {
        if (!force) {
            try {
                JobRecord rootJobRecord = queryJob(rootJobKey, JobRecord.InflationType.NONE);
                switch (rootJobRecord.getState()) {
                    case FINALIZED:
                    case STOPPED:
                        break;
                    default:
                        throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
                }
            } catch (NoSuchObjectException ex) {
                // Consider missing rootJobRecord as a non-active job and allow further delete
            }
        }
        if (async) {
            // We do all the checks above before bothering to enqueue a task.
            // They will have to be done again when the task is processed.
            DeletePipelineTask task = new DeletePipelineTask(rootJobKey, force, new QueueSettings());
            taskQueue.enqueue(task);
            return;
        }
        deleteAll(JobRecord.DATA_STORE_KIND, rootJobKey);
        deleteAll(Slot.DATA_STORE_KIND, rootJobKey);
        deleteAll(Barrier.DATA_STORE_KIND, rootJobKey);
        deleteAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
        deleteAll(FanoutTaskRecord.DATA_STORE_KIND, rootJobKey);
    }

    private static class MutationsAndBlobs {
        private final List<Mutation> mutations = Lists.newArrayList();
        private final List<PipelineMutation.BlobMutation> blobs = Lists.newArrayList();

        public void addMutation(Mutation mutation) {
            mutations.add(mutation);
        }

        public void addBlob(PipelineMutation.BlobMutation blob) {
            blobs.add(blob);
        }

        public List<Mutation> getMutations() {
            return mutations;
        }

        public List<PipelineMutation.BlobMutation> getBlobs() {
            return blobs;
        }
    }

    private abstract class Operation<R> implements Callable<R> {

        private final String name;

        Operation(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
