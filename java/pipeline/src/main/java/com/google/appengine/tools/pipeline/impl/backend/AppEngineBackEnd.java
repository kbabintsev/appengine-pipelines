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
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.appengine.tools.pipeline.impl.model.PipelineRecord;
import com.google.appengine.tools.pipeline.impl.model.Record;
import com.google.appengine.tools.pipeline.impl.model.RecordKey;
import com.google.appengine.tools.pipeline.impl.model.Slot;
import com.google.appengine.tools.pipeline.impl.model.StatusMessages;
import com.google.appengine.tools.pipeline.impl.tasks.DeletePipelineTask;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.TestUtils;
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import com.google.appengine.tools.pipeline.impl.util.ValueStoragePath;
import com.google.cloud.ExceptionHandler;
import com.google.cloud.RetryHelper;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
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

    private boolean transactionallySaveAll(final UpdateSpec.Transaction transactionSpec,
                                           final QueueSettings queueSettings, final UUID pipelineKey, final UUID jobKey, final JobRecord.State... expectedStates) {
        final Mutations mutations = new Mutations(transactionSpec);
        mutations.saveBlobs();
        final Boolean out;
        if (mutations.hasSomethingToBuffer()) {
            out = databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Boolean>() {
                @Nullable
                @Override
                public Boolean run(final TransactionContext transaction) {
                    if (jobKey != null && expectedStates != null) {
                        final Struct entity = transaction.readRow(JobRecord.DATA_STORE_KIND, Key.of(pipelineKey.toString(), jobKey.toString()), ImmutableList.of(JobRecord.STATE_PROPERTY));
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
                    mutations.bufferInto(transaction);
                    return true;
                }
            });
        } else {
            out = true;
        }
        if (transactionSpec instanceof UpdateSpec.TransactionWithTasks) {
            final UpdateSpec.TransactionWithTasks transactionWithTasks =
                    (UpdateSpec.TransactionWithTasks) transactionSpec;
            final Collection<Task> tasks = transactionWithTasks.getTasks();
            if (tasks.size() > 0) {
                final byte[] encodedTasks = FanoutTask.encodeTasks(tasks);
                final FanoutTaskRecord ftRecord = new FanoutTaskRecord(pipelineKey, encodedTasks);
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
                final FanoutTask fannoutTask = new FanoutTask(pipelineKey, ftRecord.getKey(), queueSettings);
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
        for (final UpdateSpec.Transaction group : updateSpec.getTransactions()) {
            final Mutations mutations = new Mutations(group);
            mutations.saveBlobs();
            if (mutations.hasSomethingToBuffer()) {
                databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(final TransactionContext transaction) {
                        mutations.bufferInto(transaction);
                        return null;
                    }
                });
            }
        }

        // TODO(user): Replace this with plug-able hooks that could be used by tests,
        // if needed could be restricted to package-scoped tests.
        // If a unit test requests us to do so, fail here.
        TestUtils.throwHereForTesting("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
        //tryFiveTimes was here
        final boolean wasSaved = transactionallySaveAll(
                updateSpec.getFinalTransaction(), queueSettings, updateSpec.getPipelineKey(), jobKey, expectedStates);
        return wasSaved;
    }

    @Override
    public void save(final UpdateSpec updateSpec, final QueueSettings queueSettings) {
        saveWithJobStateCheck(updateSpec, queueSettings, null);
    }

    @Override
    public JobRecord queryJob(final UUID pipelineKey, final UUID jobKey, final JobRecord.InflationType inflationType)
            throws NoSuchObjectException {
        final Struct entity = getEntity("queryJob", JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, pipelineKey, jobKey);
        final JobRecord jobRecord = new JobRecord(null, entity);
        Barrier runBarrier = null;
        Barrier finalizeBarrier = null;
        Slot outputSlot = null;
        JobInstanceRecord jobInstanceRecord = null;
        ExceptionRecord failureRecord = null;
        switch (inflationType) {
            case FOR_RUN:
                runBarrier = queryBarrier(pipelineKey, jobRecord.getRunBarrierKey(), true);
                finalizeBarrier = queryBarrier(pipelineKey, jobRecord.getFinalizeBarrierKey(), false);
                jobInstanceRecord =
                        new JobInstanceRecord(
                                pipelineManager.get(),
                                null,
                                getEntity("queryJob", JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, pipelineKey, jobRecord.getJobInstanceKey())
                        );
                outputSlot = querySlot(pipelineKey, jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_FINALIZE:
                finalizeBarrier = queryBarrier(pipelineKey, jobRecord.getFinalizeBarrierKey(), true);
                outputSlot = querySlot(pipelineKey, jobRecord.getOutputSlotKey(), false);
                break;
            case FOR_OUTPUT:
                outputSlot = querySlot(pipelineKey, jobRecord.getOutputSlotKey(), false);
                final UUID failureKey = jobRecord.getExceptionKey();
                failureRecord = queryFailure(pipelineKey, failureKey);
                break;
            default:
        }
        jobRecord.inflate(runBarrier, finalizeBarrier, outputSlot, jobInstanceRecord, failureRecord);
        LOGGER.finest("Query returned: " + jobRecord);
        return jobRecord;
    }

    private List<PipelineRecord> queryPipelines(
            @Nullable final UUID pipelineKey,
            @Nullable final String classFilter,
            @Nullable final String displayNameFilter,
            @Nullable final Set<JobRecord.State> inStates,
            final int limit,
            final int offset,
            final boolean queryOutputSlot,
            final boolean queryException
    ) {
        final String pipelinePrefix = PipelineRecord.DATA_STORE_KIND + "_";
        final String jobPrefix = JobRecord.DATA_STORE_KIND + "_";
        final String exceptionPrefix = ExceptionRecord.DATA_STORE_KIND + "_";
        final String slotPrefix = Slot.DATA_STORE_KIND + "_";
        // fields
        final ImmutableList.Builder<String> fields = ImmutableList.<String>builder()
                .addAll(PipelineRecord.propertiesForSelect(pipelinePrefix))
                .addAll(JobRecord.propertiesForSelect(jobPrefix));
        if (queryOutputSlot) {
            fields.addAll(ExceptionRecord.propertiesForSelect(exceptionPrefix));
        }
        if (queryException) {
            fields.addAll(Slot.propertiesForSelect(slotPrefix));
        }
        // statement
        final Statement.Builder statement = Statement.newBuilder("SELECT "
                + String.join(",", fields.build()) + " "
                + "FROM " + PipelineRecord.DATA_STORE_KIND + " "
                + "JOIN " + JobRecord.DATA_STORE_KIND + " "
                + "ON " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " = " + JobRecord.DATA_STORE_KIND + "." + JobRecord.PIPELINE_KEY_PROPERTY + " "
                + "AND " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " = " + JobRecord.DATA_STORE_KIND + "." + JobRecord.KEY_PROPERTY + " ");
        if (queryOutputSlot) {
            statement.append("FULL JOIN " + Slot.DATA_STORE_KIND + " "
                    + "ON " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " = " + Slot.DATA_STORE_KIND + "." + Slot.PIPELINE_KEY_PROPERTY + " "
                    + "AND " + JobRecord.DATA_STORE_KIND + "." + JobRecord.OUTPUT_SLOT_PROPERTY + " = " + Slot.DATA_STORE_KIND + "." + Slot.KEY_PROPERTY + " ");
        }
        if (queryException) {
            statement.append("FULL JOIN " + ExceptionRecord.DATA_STORE_KIND + " "
                    + "ON " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " = " + ExceptionRecord.DATA_STORE_KIND + "." + ExceptionRecord.PIPELINE_KEY_PROPERTY + " "
                    + "AND " + JobRecord.DATA_STORE_KIND + "." + JobRecord.EXCEPTION_KEY_PROPERTY + " = " + ExceptionRecord.DATA_STORE_KIND + "." + ExceptionRecord.KEY_PROPERTY + " ");
        }
        if (pipelineKey != null) {
            statement.append("WHERE " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " = @pipelineKey ")
                    .bind("pipelineKey").to(pipelineKey.toString());
        } else if (classFilter != null) {
            statement.append("WHERE " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.ROOT_JOB_CLASS_NAME + " = @classFilter ")
                    .bind("classFilter").to(classFilter);
        } else if (displayNameFilter != null) {
            statement.append("WHERE " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.ROOT_JOB_DISPLAY_NAME + " = @displayNameFilter ")
                    .bind("displayNameFilter").to(displayNameFilter);
        } else {
            statement.append("WHERE true ");
        }
        if (inStates != null && !inStates.isEmpty()) {
            statement.append("AND " + JobRecord.DATA_STORE_KIND + "." + JobRecord.STATE_PROPERTY + " IN UNNEST(@states) ")
                    .bind("states").toStringArray(inStates.stream().map(JobRecord.State::toString).collect(Collectors.toList()));
        }
        if (UuidGenerator.isTest()) {
            statement.append("AND " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.PIPELINE_KEY_PROPERTY + " LIKE @prefix ")
                    .bind("prefix").to(UuidGenerator.getTestPrefix() + "%");
        }
        statement.append("ORDER BY " + PipelineRecord.DATA_STORE_KIND + "." + PipelineRecord.ROOT_JOB_CLASS_NAME + " ");
        if (limit > 0) {
            statement.append("LIMIT @limit ")
                    .bind("limit").to(limit);
        }
        if (offset > 0) {
            statement.append("OFFSET @offset ")
                    .bind("offset").to(offset);
        }
        // execution
        final List<PipelineRecord> out = Lists.newArrayList();
        try (ResultSet rs = databaseClient.singleUse().executeQuery(statement.build())) {
            while (rs.next()) {
                final PipelineRecord pipelineRecord = new PipelineRecord(pipelinePrefix, rs);
                final JobRecord jobRecord = new JobRecord(jobPrefix, rs);
                final Slot outputSlot = !queryOutputSlot ? null : new Slot(pipelineManager.get(), slotPrefix, rs, false);
                final ExceptionRecord exceptionRecord = !queryException || ExceptionRecord.isNullInJoin(exceptionPrefix, rs)
                        ? null
                        : new ExceptionRecord(pipelineManager.get(), exceptionPrefix, rs);
                jobRecord.inflate(null, null, outputSlot, null, exceptionRecord);
                pipelineRecord.inflateRootJob(jobRecord);
                out.add(pipelineRecord);
            }
        }
        return out;
    }

    @Override
    public PipelineRecord queryPipeline(final UUID pipelineKey) throws NoSuchObjectException {
        final List<PipelineRecord> pipelineRecords = queryPipelines(pipelineKey, null, null, null, 1, 0, true, true);
        if (pipelineRecords.isEmpty()) {
            throw new NoSuchObjectException(PipelineRecord.DATA_STORE_KIND, pipelineKey, pipelineKey);
        }
        return pipelineRecords.get(0);
    }

    /**
     * {@code inflate = true} means that {@link Barrier#getWaitingOnInflated()}
     * will not return {@code null}.
     */
    private Barrier queryBarrier(final UUID pipelineKey, final UUID barrierKey, final boolean inflate) throws NoSuchObjectException {
        final Struct entity = getEntity("queryBarrier", Barrier.DATA_STORE_KIND, Barrier.PROPERTIES, pipelineKey, barrierKey);
        final Barrier barrier = new Barrier(null, entity);
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
     * @param barriers Barrier to fill slots
     */
    private void inflateBarriers(final Collection<Barrier> barriers) {
        // Step 1. Build the set of keys corresponding to the slots.
        final Set<RecordKey> keySet = new HashSet<>(barriers.size() * 5);
        for (final Barrier barrier : barriers) {
            keySet.addAll(barrier.getWaitingOnKeys());
        }
        // Step 2. Query the datastore for the Slot entities
        // Step 3. Convert into map from key to Slot
        final Map<RecordKey, Slot> slotMap = new HashMap<>(keySet.size());
        getEntities(
                "inflateBarriers",
                Slot.DATA_STORE_KIND,
                Slot.PROPERTIES,
                keySet,
                struct -> {
                    slotMap.put(
                            new RecordKey(UUID.fromString(struct.getString(Slot.PIPELINE_KEY_PROPERTY)), UUID.fromString(struct.getString(Slot.KEY_PROPERTY))),
                            new Slot(pipelineManager.get(), null, struct)
                    );
                }
        );

        // Step 4. Inflate each of the barriers
        for (final Barrier barrier : barriers) {
            barrier.inflate(slotMap);
        }
    }

    @Override
    public Slot querySlot(final UUID pipelineKey, final UUID slotKey, final boolean inflate) throws NoSuchObjectException {
        final Struct entity = getEntity("querySlot", Slot.DATA_STORE_KIND, Slot.PROPERTIES, pipelineKey, slotKey);
        final Slot slot = new Slot(pipelineManager.get(), null, entity);
        if (inflate) {
            final Map<RecordKey, Barrier> barriers = new HashMap<>();
            getEntities(
                    "querySlot",
                    Barrier.DATA_STORE_KIND,
                    Barrier.PROPERTIES,
                    slot.getWaitingOnMeKeys(),
                    struct -> {
                        barriers.put(
                                new RecordKey(
                                        UUID.fromString(struct.getString(Barrier.PIPELINE_KEY_PROPERTY)),
                                        UUID.fromString(struct.getString(Barrier.KEY_PROPERTY))
                                ),
                                new Barrier(null, struct)
                        );
                    }
            );
            slot.inflate(barriers);
            inflateBarriers(barriers.values());
        }
        return slot;
    }

    @Override
    public ExceptionRecord queryFailure(final UUID pipelineKey, final UUID failureKey) throws NoSuchObjectException {
        if (failureKey == null) {
            return null;
        }
        final Struct entity = getEntity("ReadExceptionRecord", ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, pipelineKey, failureKey);
        return new ExceptionRecord(pipelineManager.get(), null, entity);
    }

    private void getEntities(final String logString, final String tableName, final List<String> columns, final Collection<RecordKey> keys, final Consumer<StructReader> consumer) {
        //tryFiveTimes was here
        final KeySet.Builder keysBuilder = KeySet.newBuilder();
        keys.forEach(key -> keysBuilder.addKey(Key.of(key.getPipelineKey().toString(), key.getKey().toString())));
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
            throw new RuntimeException("Missing " + (keys.size() - count) + " entities");
        }
    }

    private Struct getEntity(final String logString, final String tableName, final List<String> columns, final UUID pipelineKey, final UUID key) throws NoSuchObjectException {
        // tryFiveTimes was here
        final Struct entity = databaseClient.singleUse().readRow(tableName, Key.of(pipelineKey.toString(), key.toString()), columns);
        if (entity == null) {
            throw new NoSuchObjectException(tableName, pipelineKey, key);
        }
        return entity;
    }

    @Override
    public void handleFanoutTask(final FanoutTask fanoutTask) throws NoSuchObjectException {
        final UUID fanoutTaskRecordKey = fanoutTask.getRecordKey();
        // Fetch the fanoutTaskRecord outside of any transaction
        final Struct entity = getEntity("handleFanoutTask", FanoutTaskRecord.DATA_STORE_KIND, FanoutTaskRecord.PROPERTIES, fanoutTask.getPipelineKey(), fanoutTaskRecordKey);
        final FanoutTaskRecord ftRecord = new FanoutTaskRecord(null, entity);
        final byte[] encodedBytes = ftRecord.getPayload();
        taskQueue.enqueue(FanoutTask.decodeTasks(encodedBytes));
    }

    public void queryAll(@Nullable final ReadContext transaction, final String tableName, final List<String> columns, final UUID pipelineKey, final Consumer<StructReader> consumer) {
        try (ResultSet rs = (transaction == null ? databaseClient.singleUse() : transaction).executeQuery(
                Statement.newBuilder(
                        "SELECT " + String.join(", ", columns) + " "
                                + "FROM " + tableName + " "
                                + "WHERE " + PipelineModelObject.PIPELINE_KEY_PROPERTY + " = @pipelineKey"
                ).bind("pipelineKey").to(pipelineKey.toString()).build()
        )) {
            {
                while (rs.next()) {
                    consumer.accept(rs);
                }
            }
        }
    }

    @Override
    public List<PipelineRecord> queryRootPipelines(
            @Nullable final String classFilter,
            @Nullable final Set<JobRecord.State> inStates,
            final int limit,
            final int offset
    ) {
        return queryPipelines(null, classFilter, null, inStates, limit, offset, false, false);
    }

    @Override
    public Set<String> getRootPipelinesClassName() {
        final Set<String> pipelines = new LinkedHashSet<>();
        final Statement.Builder builder = Statement.newBuilder(
                "SELECT DISTINCT " + PipelineRecord.ROOT_JOB_CLASS_NAME + " "
                        + "FROM " + PipelineRecord.DATA_STORE_KIND + " "
                        + "WHERE " + PipelineRecord.ROOT_JOB_CLASS_NAME + " IS NOT null "

        );
        if (UuidGenerator.isTest()) {
            builder.append("AND " + PipelineRecord.PIPELINE_KEY_PROPERTY + " LIKE @prefix ")
                    .bind("prefix").to(UuidGenerator.getTestPrefix() + "%");
        }
        builder.append("ORDER BY " + PipelineRecord.ROOT_JOB_CLASS_NAME);
        try (ResultSet rs = databaseClient.singleUse().executeQuery(builder.build())) {
            {
                while (rs.next()) {
                    pipelines.add(rs.getString(PipelineRecord.ROOT_JOB_CLASS_NAME));
                }
            }
        }
        return pipelines;
    }

    @Override
    public Set<String> getRootPipelinesDisplayName() {
        final Set<String> pipelines = new LinkedHashSet<>();
        final Statement.Builder builder = Statement.newBuilder(
                "SELECT DISTINCT " + PipelineRecord.ROOT_JOB_DISPLAY_NAME + " "
                        + "FROM " + PipelineRecord.DATA_STORE_KIND + " "
                        + "WHERE " + PipelineRecord.ROOT_JOB_DISPLAY_NAME + " IS NOT null "

        );
        if (UuidGenerator.isTest()) {
            builder.append("AND " + PipelineRecord.PIPELINE_KEY_PROPERTY + " LIKE @prefix ")
                    .bind("prefix").to(UuidGenerator.getTestPrefix() + "%");
        }
        builder.append("ORDER BY " + PipelineRecord.ROOT_JOB_DISPLAY_NAME);
        try (ResultSet rs = databaseClient.singleUse().executeQuery(builder.build())) {
            {
                while (rs.next()) {
                    pipelines.add(rs.getString(PipelineRecord.ROOT_JOB_DISPLAY_NAME));
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
                        "SELECT DISTINCT " + JobRecord.PIPELINE_KEY_PROPERTY + " "
                                + "FROM " + JobRecord.DATA_STORE_KIND + " "
                                + "WHERE " + JobRecord.PIPELINE_KEY_PROPERTY + " LIKE @prefix"
                ).bind("prefix").to(prefix + "%").build()
        )) {
            {
                while (rs.next()) {
                    pipelines.add(UUID.fromString(rs.getString(JobRecord.PIPELINE_KEY_PROPERTY)));
                }
            }
        }
        LOGGER.info("Retrieved " + pipelines.size() + " test pipelines with prefix " + prefix);
        return pipelines;
    }

    @Override
    public PipelineObjects queryFullPipeline(final UUID pipelineKey) {
        final Map<UUID, JobRecord> jobs = new HashMap<>();
        final Map<RecordKey, Slot> slots = new HashMap<>();
        final Map<RecordKey, Barrier> barriers = new HashMap<>();
        final Map<UUID, JobInstanceRecord> jobInstanceRecords = new HashMap<>();
        final Map<UUID, ExceptionRecord> failureRecords = new HashMap<>();

        final PipelineRecord pipeline = new PipelineRecord(null, databaseClient.singleUse().readRow(PipelineRecord.DATA_STORE_KIND, Key.of(pipelineKey.toString()), PipelineRecord.PROPERTIES));
        try (ReadContext transaction = databaseClient.readOnlyTransaction()) {
            queryAll(transaction, Barrier.DATA_STORE_KIND, Barrier.PROPERTIES, pipelineKey, (struct) -> {
                barriers.put(
                        new RecordKey(UUID.fromString(struct.getString(Barrier.PIPELINE_KEY_PROPERTY)), UUID.fromString(struct.getString(Barrier.KEY_PROPERTY))),
                        new Barrier(null, struct)
                );
            });
            queryAll(transaction, Slot.DATA_STORE_KIND, Slot.PROPERTIES, pipelineKey, (struct) -> {
                slots.put(
                        new RecordKey(UUID.fromString(struct.getString(Slot.PIPELINE_KEY_PROPERTY)), UUID.fromString(struct.getString(Slot.KEY_PROPERTY))),
                        new Slot(pipelineManager.get(), null, struct, true)
                );
            });
            queryAll(transaction, JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, pipelineKey, (struct) -> {
                jobs.put(UUID.fromString(struct.getString(JobRecord.KEY_PROPERTY)), new JobRecord(null, struct));
            });
            queryAll(transaction, JobInstanceRecord.DATA_STORE_KIND, JobInstanceRecord.PROPERTIES, pipelineKey, (struct) -> {
                jobInstanceRecords.put(UUID.fromString(struct.getString(JobInstanceRecord.KEY_PROPERTY)), new JobInstanceRecord(pipelineManager.get(), null, struct));
            });
            queryAll(transaction, ExceptionRecord.DATA_STORE_KIND, ExceptionRecord.PROPERTIES, pipelineKey, (struct) -> {
                failureRecords.put(UUID.fromString(struct.getString(ExceptionRecord.KEY_PROPERTY)), new ExceptionRecord(pipelineManager.get(), null, struct));
            });
        }
        return new PipelineObjects(pipelineKey, pipeline, jobs, slots, barriers, jobInstanceRecords, failureRecords);
    }

    /**
     * Delete all datastore entities corresponding to the given pipeline.
     *
     * @param pipelineKey The root job key identifying the pipeline
     * @param force       If this parameter is not {@code true} then this method will
     *                    throw an {@link IllegalStateException} if the specified pipeline is not in the
     *                    {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
     *                    {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
     * @param async       If this parameter is {@code true} then instead of performing
     *                    the delete operation synchronously, this method will enqueue a task
     *                    to perform the operation.
     * @throws IllegalStateException If {@code force = false} and the specified
     *                               pipeline is not in the
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#FINALIZED} or
     *                               {@link com.google.appengine.tools.pipeline.impl.model.JobRecord.State#STOPPED} state.
     */
    @Override
    public void deletePipeline(final UUID pipelineKey, final boolean force, final boolean async) throws IllegalStateException {
        if (!force) {
            try {
                final JobRecord rootJobRecord = queryJob(pipelineKey, pipelineKey, JobRecord.InflationType.NONE);
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
            final DeletePipelineTask task = new DeletePipelineTask(pipelineKey, force, new QueueSettings());
            taskQueue.enqueue(task);
            return;
        }
        databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<Void>() {
            @Nullable
            @Override
            public Void run(final TransactionContext transaction) {
                final long deleted = transaction.executeUpdate(
                        Statement.newBuilder("DELETE FROM " + PipelineRecord.DATA_STORE_KIND + " WHERE " + PipelineRecord.PIPELINE_KEY_PROPERTY + " = @pipelineKey")
                                .bind("pipelineKey").to(pipelineKey.toString())
                                .build()
                );
                return null;
            }
        });
    }

    @Override
    public void addStatusMessage(final UUID pipelineKey, final UUID jobKey, final int attemptNumber, final String message) throws NoSuchObjectException {
        final NoSuchObjectException ex = databaseClient.readWriteTransaction().run(new TransactionRunner.TransactionCallable<NoSuchObjectException>() {
            @Nullable
            @Override
            public NoSuchObjectException run(final TransactionContext transaction) throws Exception {
                final Struct entity = transaction.readRow(JobRecord.DATA_STORE_KIND, Key.of(pipelineKey.toString(), jobKey.toString()), ImmutableList.of(JobRecord.STATUS_MESSAGES));
                if (entity == null) {
                    return new NoSuchObjectException(JobRecord.DATA_STORE_KIND, pipelineKey, jobKey);
                }
                final StatusMessages statusMessages = new StatusMessages(
                        JobRecord.STATUS_MESSAGES_MAX_COUNT,
                        JobRecord.STATUS_MESSAGES_MAX_LENGTH,
                        entity.isNull(JobRecord.STATUS_MESSAGES)
                                ? Lists.newArrayList()
                                : entity.getStringList(JobRecord.STATUS_MESSAGES)
                );
                statusMessages.add(attemptNumber, "Ext", message);
                transaction.buffer(
                        Mutation.newUpdateBuilder(JobRecord.DATA_STORE_KIND)
                                .set(JobRecord.PIPELINE_KEY_PROPERTY).to(pipelineKey.toString())
                                .set(JobRecord.KEY_PROPERTY).to(jobKey.toString())
                                .set(JobRecord.STATUS_MESSAGES).toStringArray(statusMessages.getAll())
                                .build()
                );
                return null;
            }
        });
        if (ex != null) {
            throw ex;
        }
    }

    private final class Mutations {

        private final String name;
        private final List<Mutation> databaseMutations = Lists.newArrayList();
        private final List<PipelineMutation.ValueMutation> valueMutations = Lists.newArrayList();

        private Mutations(final UpdateSpec.Transaction group) {
            this.name = group.getName();
            addAllOneType(group.getPipelines());
            addAllOneType(group.getBarriers());
            addAllOneType(group.getSlots());
            addAllOneType(group.getFailureRecords());
            addAllOneType(group.getJobs());
            addAllOneType(group.getJobInstanceRecords());
        }

        private void addAllOneType(final Collection<? extends Record> objects) {
            if (objects.isEmpty()) {
                return;
            }
            for (final Record x : objects) {
                final PipelineMutation m = x.toEntity();
                final Mutation mutation = m.getDatabaseMutation().build();
                databaseMutations.add(mutation);
                if (m.getValueMutation() != null) {
                    valueMutations.add(m.getValueMutation());
                }
            }
        }

        public void saveBlobs() {
            if (!valueMutations.isEmpty()) {
                LOGGER.finest("Mutations: '" + name + "' saving " + valueMutations.size() + " storage blobs");
                for (final PipelineMutation.ValueMutation valueMutation : valueMutations) {
                    saveBlob(valueMutation.getPath(), valueMutation.getValue());
                }
            }
        }

        public boolean hasSomethingToBuffer() {
            return !databaseMutations.isEmpty();
        }

        public void bufferInto(final TransactionContext transaction) {
            LOGGER.finest("Mutations: '" + name + "' saving " + databaseMutations.size() + " database rows");
            transaction.buffer(databaseMutations);
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
