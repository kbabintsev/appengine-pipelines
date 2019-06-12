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
import com.google.api.core.CurrentMillisClock;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.appengine.tools.pipeline.Consts;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
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
import com.google.appengine.tools.pipeline.impl.model.ValueStoragePath;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.TestUtils;
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import com.google.appengine.tools.pipeline.util.Pair;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.RetryHelper;
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
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.threeten.bp.Duration;

import javax.annotation.Nullable;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import java.util.stream.Collectors;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
@Singleton
public final class AppEngineBackEnd implements PipelineBackEnd {

    private static final int INITIAL_RETRY_DELAY_MILLIS = 300;
    private static final int MAX_RETRY_DELAY_MILLIS = 5000;
    private static final int HTTP_NOT_FOUND = 404;

    //    private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
//            ConcurrentModificationException.class, DatastoreTimeoutException.class,
//            DatastoreFailureException.class)
//            .abortOn(EntityNotFoundException.class, NoSuchObjectException.class).build();
    private static final RetrySettings RETRY_SETTINGS = RetrySettings.newBuilder()
            .setRetryDelayMultiplier(2)
            .setInitialRetryDelay(Duration.ofMillis(INITIAL_RETRY_DELAY_MILLIS))
            .setMaxRetryDelay(Duration.ofMillis(MAX_RETRY_DELAY_MILLIS))
            .setMaxAttempts(5)
            .build();
    private static final ExceptionHandler STORAGE_EXCEPTION_HANDLER = ExceptionHandler.newBuilder()
            .retryOn(IOException.class)
            .abortOn(RuntimeException.class)
            .build();
    private static final Logger LOGGER = Logger.getLogger(AppEngineBackEnd.class.getName());
    private final Spanner spanner;
    private final DatabaseClient databaseClient;
    private final PipelineTaskQueue taskQueue;
    private final Storage storage;
    private final Provider<PipelineManager> pipelineManager;

    @Inject
    public AppEngineBackEnd(final Provider<PipelineManager> pipelineManager, final PipelineTaskQueue pipelineTaskQueue) {
        this.pipelineManager = pipelineManager;
        spanner = SpannerOptions.newBuilder().build().getService();
        final DatabaseId logsId = DatabaseId.of(Consts.SPANNER_PROJECT, Consts.SPANNER_INSTANCE, Consts.SPANNER_DATABASE);
        databaseClient = spanner.getDatabaseClient(logsId);
        taskQueue = pipelineTaskQueue;
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

    private void addAll(final Collection<? extends PipelineModelObject> objects, final Mutations allMutations, final boolean runTransaction) {
        if (objects.isEmpty()) {
            return;
        }
        for (final PipelineModelObject x : objects) {
            LOGGER.finest("Storing: " + x);
            final PipelineMutation m = x.toEntity();
            final Mutation mutation = m.getDatabaseMutation().build();
            allMutations.addDatabaseMutation(mutation);
            if (m.getValueMutation() != null) {
                allMutations.addValueMutation(m.getValueMutation());
            }

            if (runTransaction) {
                databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(final TransactionContext transaction) {
                        transaction.buffer(mutation);
                        return null;
                    }
                });
            }
        }
    }

    private void addAll(final UpdateSpec.Group group, final Mutations allMutations, final boolean runTransaction) {
        addAll(group.getBarriers(), allMutations, runTransaction);
        addAll(group.getJobs(), allMutations, runTransaction);
        addAll(group.getSlots(), allMutations, runTransaction);
        addAll(group.getJobInstanceRecords(), allMutations, runTransaction);
        addAll(group.getFailureRecords(), allMutations, runTransaction);
    }

    private void saveAll(final UpdateSpec.Group transactionSpec, @Nullable final TransactionContext parentTransaction) {
        final Mutations allMutations = new Mutations();
        addAll(transactionSpec, allMutations, parentTransaction == null);
        for (final PipelineMutation.ValueMutation valueMutation : allMutations.getValueMutations()) {
            saveBlob(valueMutation.getPath(), valueMutation.getValue());
        }

        if (parentTransaction != null) {
            parentTransaction.buffer(allMutations.getDatabaseMutations());
        }
    }

    private boolean transactionallySaveAll(final UpdateSpec.Transaction transactionSpec,
                                           final QueueSettings queueSettings, final UUID rootJobKey, final UUID jobKey, final JobRecord.State... expectedStates) {
        final Boolean out = databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Boolean>() {
            @Nullable
            @Override
            public Boolean run(final TransactionContext transaction) {
                if (jobKey != null && expectedStates != null) {
                    final Struct entity = transaction.readRow(JobRecord.DATA_STORE_KIND, Key.of(jobKey.toString()), ImmutableList.of(JobRecord.STATE_PROPERTY));
                    if (entity == null) {
                        throw new RuntimeException(
                                "Fatal Pipeline corruption error. No JobRecord found with key = " + jobKey);
                    }
                    final JobRecord.State state = JobRecord.State.valueOf(entity.getString(JobRecord.STATE_PROPERTY));
                    boolean stateIsExpected = false;
                    for (final JobRecord.State expectedState : expectedStates) {
                        if (state == expectedState) {
                            stateIsExpected = true;
                            break;
                        }
                    }
                    if (!stateIsExpected) {
                        LOGGER.info("Job " + jobKey + " is not in one of the expected states: "
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
            final UpdateSpec.TransactionWithTasks transactionWithTasks =
                    (UpdateSpec.TransactionWithTasks) transactionSpec;
            final Collection<Task> tasks = transactionWithTasks.getTasks();
            if (tasks.size() > 0) {
                final byte[] encodedTasks = FanoutTask.encodeTasks(tasks);
                final FanoutTaskRecord ftRecord = new FanoutTaskRecord(rootJobKey, encodedTasks);
                // Store FanoutTaskRecord outside of any transaction, but before
                // the FanoutTask is enqueued. If the put succeeds but the
                // enqueue fails then the FanoutTaskRecord is orphaned. But
                // the Pipeline is still consistent.
                databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(final TransactionContext transaction2) {
                        transaction2.buffer(ImmutableList.of(ftRecord.toEntity().getDatabaseMutation().build()));
                        return null;
                    }
                });
                final FanoutTask fannoutTask = new FanoutTask(ftRecord.getKey(), queueSettings);
                taskQueue.enqueue(fannoutTask);
            }
        }
        return out == null ? false : out;
    }

    private <R> R tryFiveTimes(final ExceptionHandler exceptionHandler, final Operation<R> operation) {
        try {
            return RetryHelper.runWithRetries(
                    operation,
                    RETRY_SETTINGS,
                    exceptionHandler,
                    CurrentMillisClock.getDefaultClock()
            );
        } catch (RetryHelper.RetryHelperException e) {
            if (e.getCause() instanceof RuntimeException) {
                LOGGER.info(e.getCause().getMessage() + " during " + operation.getName()
                        + " throwing after multiple attempts ");
                throw (RuntimeException) e.getCause();
            } else {
                throw e;
            }
        }
    }

    @Override
    public void cleanBlobs(final String prefix) {
        while (true) {
            final Objects objects = tryFiveTimes(STORAGE_EXCEPTION_HANDLER, new Operation<Objects>("storage.objects.list") {
                @Override
                public Objects call() throws IOException {
                    return storage.objects().list(Consts.BLOB_BUCKET_NAME).setPrefix(prefix).execute();
                }
            });

            if (objects == null || objects.getItems() == null || objects.getItems().isEmpty()) {
                break;
            }
            LOGGER.finest("Deleting " + objects.getItems().size() + " valueMutations from prefix " + prefix);
            for (final StorageObject object : objects.getItems()) {
                tryFiveTimes(STORAGE_EXCEPTION_HANDLER, new Operation<Void>("storage.objects.delete") {
                    @Override
                    public Void call() throws IOException {
                        storage.objects().delete(object.getBucket(), object.getName()).execute();
                        return null;
                    }
                });
            }
        }
    }

    @Override
    public void saveBlob(final ValueStoragePath valueStoragePath, final byte[] value) {
        final String path = valueStoragePath.getPath();
        LOGGER.finest("Saving value " + path + " size of " + value.length + "...");
        tryFiveTimes(STORAGE_EXCEPTION_HANDLER, new Operation<Void>("storage.objects.insert") {
            @Override
            public Void call() throws IOException {
                storage.objects().insert(
                        Consts.BLOB_BUCKET_NAME,
                        new StorageObject()
                                .setBucket(Consts.BLOB_BUCKET_NAME)
                                .setName(path),
                        new ByteArrayContent("application/octet-stream", value)
                ).execute();
                LOGGER.finest("Saved object " + path);
                return null;
            }
        });
    }

    @Override
    public byte[] retrieveBlob(final ValueStoragePath valueStoragePath) {
        final String path = valueStoragePath.getPath();
        try {
            LOGGER.finest("Retrieving blob " + path + "...");
            final InputStream inputStream = tryFiveTimes(STORAGE_EXCEPTION_HANDLER, new Operation<InputStream>("storage.objects.get") {
                @Override
                public InputStream call() throws IOException {
                    return storage.objects().get(
                            Consts.BLOB_BUCKET_NAME,
                            path
                    ).executeMediaAsInputStream();
                }
            });
            final byte[] out = ByteStreams.toByteArray(inputStream);
            LOGGER.finest("Retrieved object " + path + " size " + out.length);
            return out;
        } catch (GoogleJsonResponseException ex) {
            if (ex.getStatusCode() == HTTP_NOT_FOUND) {
                LOGGER.finest("Blob " + path + " returned 404 status");
                return null;
            } else {
                throw new RuntimeException("Can't retrieve object from Storage bucket", ex);
            }
        } catch (IOException ex) {
            throw new RuntimeException("Can't retrieve object from Storage bucket", ex);
        }
    }

    @Override
    public void enqueue(final Task task) {
        taskQueue.enqueue(task);
    }

    @Override
    public boolean saveWithJobStateCheck(final UpdateSpec updateSpec,
                                         final QueueSettings queueSettings, final UUID jobKey,
                                         final JobRecord.State... expectedStates) {
        // tryFiveTimes was here
        saveAll(updateSpec.getNonTransactionalGroup(), null);
        for (final UpdateSpec.Transaction transactionSpec : updateSpec.getTransactions()) {
            //tryFiveTimes was here
            transactionallySaveAll(transactionSpec, queueSettings, updateSpec.getRootJobKey(), null);
        }

        // TODO(user): Replace this with plug-able hooks that could be used by tests,
        // if needed could be restricted to package-scoped tests.
        // If a unit test requests us to do so, fail here.
        TestUtils.throwHereForTesting("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
        //tryFiveTimes was here
        final boolean wasSaved = transactionallySaveAll(
                updateSpec.getFinalTransaction(), queueSettings, updateSpec.getRootJobKey(), jobKey, expectedStates);
        return wasSaved;
    }

    @Override
    public void save(final UpdateSpec updateSpec, final QueueSettings queueSettings) {
        saveWithJobStateCheck(updateSpec, queueSettings, null);
    }

    @Override
    public JobRecord queryJob(final UUID jobKey, final JobRecord.InflationType inflationType)
            throws NoSuchObjectException {
        final Struct entity = getEntity("queryJob", JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, jobKey);
        final JobRecord jobRecord = new JobRecord(entity);
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
                        new JobInstanceRecord(
                                pipelineManager.get(),
                                getEntity("queryJob", JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, jobRecord.getJobInstanceKey())
                        );
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_FINALIZE:
                finalizeBarrier = queryBarrier(jobRecord.getFinalizeBarrierKey(), true);
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_OUTPUT:
                outputSlot = querySlot(jobRecord.getOutputSlotKey(), false);
                final UUID failureKey = jobRecord.getExceptionKey();
                failureRecord = queryFailure(failureKey);
                break;
            default:
        }
        jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
        LOGGER.finest("Query returned: " + jobRecord);
        return jobRecord;
    }

    /**
     * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
     * will not return {@code null}.
     */
    private Barrier queryBarrier(final UUID barrierKey, final boolean inflate) throws NoSuchObjectException {
        final Struct entity = getEntity("queryBarrier", Barrier.DATA_STORE_KIND, Barrier.PROPERTIES, barrierKey);
        final Barrier barrier = new Barrier(entity);
        if (inflate) {
            final Collection<Barrier> barriers = new ArrayList<>(1);
            barriers.add(barrier);
            inflateBarriers(barriers);
        }
        LOGGER.finest("Querying returned: " + barrier);
        return barrier;
    }

    /**
     * Given a {@link Collection} of {@link Barrier Barriers}, inflate each of the
     * {@link Barrier Barriers} so that {@link Barrier#getWaitingOnInflated()}
     * will not return null;
     *
     * @param barriers
     */
    private void inflateBarriers(final Collection<Barrier> barriers) {
        // Step 1. Build the set of keys corresponding to the slots.
        final Set<UUID> keySet = new HashSet<>(barriers.size() * 5);
        for (final Barrier barrier : barriers) {
            keySet.addAll(barrier.getWaitingOnKeys());
        }
        // Step 2. Query the datastore for the Slot entities
        // Step 3. Convert into map from key to Slot
        final Map<UUID, Slot> slotMap = new HashMap<>(keySet.size());
        getEntities(
                "inflateBarriers",
                Slot.DATA_STORE_KIND,
                Slot.PROPERTIES,
                keySet,
                struct -> {
                    slotMap.put(UUID.fromString(struct.getString(Slot.ID_PROPERTY)), new Slot(pipelineManager.get(), struct));
                }
        );

        // Step 4. Inflate each of the barriers
        for (final Barrier barrier : barriers) {
            barrier.inflate(slotMap);
        }
    }

    @Override
    public Slot querySlot(final UUID slotKey, final boolean inflate) throws NoSuchObjectException {
        final Struct entity = getEntity("querySlot", Slot.DATA_STORE_KIND, Slot.PROPERTIES, slotKey);
        final Slot slot = new Slot(pipelineManager.get(), entity);
        if (inflate) {
            final Map<UUID, Barrier> barriers = new HashMap<>();
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
    public ExceptionRecord queryFailure(final UUID failureKey) throws NoSuchObjectException {
        if (failureKey == null) {
            return null;
        }
        final Struct entity = getEntity("ReadExceptionRecord", ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, failureKey);
        return new ExceptionRecord(pipelineManager.get(), entity);
    }

    private void getEntities(final String logString, final String tableName, final List<String> columns, final Collection<UUID> keys, final Consumer<StructReader> consumer) {
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

    private Struct getEntity(final String logString, final String tableName, final List<String> columns, final UUID key) throws NoSuchObjectException {
        // tryFiveTimes was here
        final Struct entity = databaseClient.singleUse().readRow(tableName, Key.of(key.toString()), columns);
        if (entity == null) {
            throw new NoSuchObjectException(key.toString());
        }
        return entity;
    }

    @Override
    public void handleFanoutTask(final FanoutTask fanoutTask) throws NoSuchObjectException {
        final UUID fanoutTaskRecordKey = fanoutTask.getRecordKey();
        // Fetch the fanoutTaskRecord outside of any transaction
        final Struct entity = getEntity("handleFanoutTask", FanoutTaskRecord.DATA_STORE_KIND, FanoutTaskRecord.PROPERTIES, fanoutTaskRecordKey);
        final FanoutTaskRecord ftRecord = new FanoutTaskRecord(entity);
        final byte[] encodedBytes = ftRecord.getPayload();
        taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
    }

    public void queryAll(final String tableName, final List<String> columns, final UUID rootJobKey, final Consumer<StructReader> consumer) {
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                Statement.newBuilder(
                        "SELECT " + String.join(", ", columns) + " "
                                + "FROM " + tableName + " "
                                + "WHERE " + PipelineModelObject.ROOT_JOB_KEY_PROPERTY + " = @rootJobKey"
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
    public Pair<? extends Iterable<JobRecord>, String> queryRootPipelines(
            @Nullable final String classFilter,
            @Nullable final Set<JobRecord.State> inStates,
            final int limit,
            final int offset
    ) {
        final Statement.Builder builder = Statement.newBuilder(
                "SELECT " + String.join(", ", JobRecord.PROPERTIES) + " "
                        + "FROM " + JobRecord.DATA_STORE_KIND + " "
                        + "WHERE "
        );
        if (classFilter == null) {
            builder.append(JobRecord.ROOT_JOB_DISPLAY_NAME + " IS NOT null ");
        } else {
            builder.append(JobRecord.ROOT_JOB_DISPLAY_NAME + " = @classFilter ")
                    .bind("classFilter").to(classFilter);
        }
        if (inStates != null && !inStates.isEmpty()) {
            builder.append("AND " + JobRecord.STATE_PROPERTY + " IN UNNEST(@states)")
                    .bind("states").toStringArray(inStates.stream().map(JobRecord.State::toString).collect(Collectors.toList()));
        }
        if (UuidGenerator.isTest()) {
            builder.append("AND " + JobRecord.ROOT_JOB_KEY_PROPERTY + " LIKE @prefix ")
                    .bind("prefix").to(UuidGenerator.getTestPrefix() + "%");
        }
        builder.append("ORDER BY " + JobRecord.ROOT_JOB_DISPLAY_NAME + " ");
        if (limit > 0) {
            builder.append("LIMIT @limit ")
                    .bind("limit").to(limit);
        }
        if (offset > 0) {
            builder.append("OFFSET @offset ")
                    .bind("offset").to(offset);
        }
        final List<JobRecord> roots = new LinkedList<>();
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
        final Set<String> pipelines = new LinkedHashSet<>();
        final Statement.Builder builder = Statement.newBuilder(
                "SELECT DISTINCT " + JobRecord.ROOT_JOB_DISPLAY_NAME + " "
                        + "FROM " + JobRecord.DATA_STORE_KIND + " "
                        + "WHERE " + JobRecord.ROOT_JOB_DISPLAY_NAME + " IS NOT null "

        );
        if (UuidGenerator.isTest()) {
            builder.append("AND " + JobRecord.ROOT_JOB_KEY_PROPERTY + " LIKE @prefix ")
                    .bind("prefix").to(UuidGenerator.getTestPrefix() + "%");
        }
        builder.append("ORDER BY " + JobRecord.ROOT_JOB_DISPLAY_NAME);
        try (ResultSet rs = databaseClient.singleUse().executeQuery(builder.build())) {
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
        final Set<UUID> pipelines = new LinkedHashSet<>();
        final String prefix = UuidGenerator.getTestPrefix();
        try (ResultSet rs = databaseClient.singleUse().executeQuery(
                Statement.newBuilder(
                        "SELECT DISTINCT " + JobRecord.ROOT_JOB_KEY_PROPERTY + " "
                                + "FROM " + JobRecord.DATA_STORE_KIND + " "
                                + "WHERE " + JobRecord.ROOT_JOB_KEY_PROPERTY + " LIKE @prefix"
                ).bind("prefix").to(prefix + "%").build()
        )) {
            {
                while (rs.next()) {
                    pipelines.add(UUID.fromString(rs.getString(JobRecord.ROOT_JOB_KEY_PROPERTY)));
                }
            }
        }
        LOGGER.info("Retrieved " + pipelines.size() + " test pipelines with prefix " + prefix);
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
            slots.put(UUID.fromString(struct.getString(Slot.ID_PROPERTY)), new Slot(pipelineManager.get(), struct, true));
        });
        queryAll(JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, rootJobKey, (struct) -> {
            jobs.put(UUID.fromString(struct.getString(JobRecord.ID_PROPERTY)), new JobRecord(struct));
        });
        queryAll(JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, rootJobKey, (struct) -> {
            jobInstanceRecords.put(UUID.fromString(struct.getString(JobInstanceRecord.ID_PROPERTY)), new JobInstanceRecord(pipelineManager.get(), struct));
        });
        queryAll(ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, rootJobKey, (struct) -> {
            failureRecords.put(UUID.fromString(struct.getString(ExceptionRecord.ID_PROPERTY)), new ExceptionRecord(pipelineManager.get(), struct));
        });
        return new PipelineObjects(
                rootJobKey, jobs, slots, barriers, jobInstanceRecords, failureRecords);
    }

    private void deleteAll(final String tableName, final UUID rootJobKey) {
        LOGGER.info("Deleting all " + tableName + " with rootJobKey=" + rootJobKey);
        databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
            @Nullable
            @Override
            public Void run(final TransactionContext transaction) {
                final long deleted = transaction.executeUpdate(
                        Statement.newBuilder("DELETE FROM " + tableName + " WHERE " + PipelineModelObject.ROOT_JOB_KEY_PROPERTY + " = @rootJobKey")
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
    public void deletePipeline(final UUID rootJobKey, final boolean force, final boolean async)
            throws IllegalStateException {
        if (!force) {
            try {
                final JobRecord rootJobRecord = queryJob(rootJobKey, JobRecord.InflationType.NONE);
                switch (rootJobRecord.getState()) {
                    case FINALIZED:
                    case STOPPED:
                        break;
                    default:
                        throw new IllegalStateException("Pipeline is still running: " + rootJobRecord);
                }
            } catch (NoSuchObjectException ignore) {
                // Consider missing rootJobRecord as a non-active job and allow further delete
            }
        }
        if (async) {
            // We do all the checks above before bothering to enqueue a task.
            // They will have to be done again when the task is processed.
            final DeletePipelineTask task = new DeletePipelineTask(rootJobKey, force, new QueueSettings());
            taskQueue.enqueue(task);
            return;
        }
        deleteAll(JobRecord.DATA_STORE_KIND, rootJobKey);
        deleteAll(Slot.DATA_STORE_KIND, rootJobKey);
        deleteAll(Barrier.DATA_STORE_KIND, rootJobKey);
        deleteAll(JobInstanceRecord.DATA_STORE_KIND, rootJobKey);
        deleteAll(FanoutTaskRecord.DATA_STORE_KIND, rootJobKey);
    }

    private static final class Mutations {
        private final List<Mutation> databaseMutations = Lists.newArrayList();
        private final List<PipelineMutation.ValueMutation> valueMutations = Lists.newArrayList();

        public void addDatabaseMutation(final Mutation mutation) {
            databaseMutations.add(mutation);
        }

        public void addValueMutation(final PipelineMutation.ValueMutation valueMutation) {
            valueMutations.add(valueMutation);
        }

        public List<Mutation> getDatabaseMutations() {
            return databaseMutations;
        }

        public List<PipelineMutation.ValueMutation> getValueMutations() {
            return valueMutations;
        }
    }

    private abstract class Operation<R> implements Callable<R> {

        private final String name;

        Operation(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
