// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.backend.AppEngineBackEnd;
import com.google.appengine.tools.pipeline.impl.model.JobRecord;
import com.google.appengine.tools.pipeline.impl.model.PipelineObjects;
import com.google.apphosting.api.ApiProxy;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static com.google.appengine.tools.pipeline.impl.util.TestUtils.getFailureProperty;

/**
 * A test of the ability of the Pipeline framework to handle orphaned jobs.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class OrphanedJobGraphTest extends PipelineTest {

    public static final int SUPPLY_VALUE_DELAY = 15000;
    private static final Logger LOGGER = Logger.getLogger(PipelineManager.class.getName());

    @Override
    protected boolean isHrdSafe() {
        return false;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        GeneratorJob.runCount.set(0);
        ChildJob.runCount.set(0);
        SupplyPromisedValueRunnable.orphanedObjectExcetionCount.set(0);
    }

    /**
     * Tests the Pipeline frameworks ability to handle a failure during the
     * handling of a RunJob task just before the final transaction.
     * <p>
     * A generator job will run, a child job graph will be persisted, but an
     * exception will be thrown before the generator job can be transactionally
     * saved. The child job graph will thus be orphaned. We test that the tasks
     * that would activate the child job graph will never get enqueued and so the
     * orphaned child job will never run. The generator job will be retried a
     * second time with the same result. The generator job will be retried a third
     * time and it will succeed. Only the third child job graph is non-orphaned
     * and should be activated by tasks and run. The orphaned jobs will be cleaned
     * up when the Pipeline is deleted.
     */
    public void testOrphanedJobGraph() throws Exception {
        doOrphanedJobGraphTest(false);
    }

    /**
     * Tests that the method
     * {@link PipelineService#submitPromisedValue(UUID, UUID, Object)} behaves
     * properly if the promise key has been orphaned.
     * <p>
     * This test is similar to {@link #testOrphanedJobGraph()} except that this
     * time the child job graph is supposed to be activated asynchronously vai a
     * promised value. We test that
     * {@link PipelineService#submitPromisedValue(UUID, UUID, Object)} will throw a
     * {@link OrphanedObjectException} when {@code submitPromisedValue()} is
     * invoked on an orphaned promise key.
     */
    public void testOrphanedJobGraphWithPromisedValue() throws Exception {
        doOrphanedJobGraphTest(true);
    }

    /**
     * Performs the common testing logic for {@link #testOrphanedJobGraph()} and
     * {@link #testOrphanedJobGraphWithPromisedValue()}.
     *
     * @param usePromisedValue Should the child job be activated via a promised
     *                         value. If this is {@code false} then the child job is activated via
     *                         an immediate value.
     */
    private void doOrphanedJobGraphTest(final boolean usePromisedValue) throws Exception {

        // Run GeneratorJob
        final UUID pipelineKey = service.startNewPipeline(new GeneratorJob(usePromisedValue));
        waitForJobToComplete(pipelineKey);

        // and wait for thread to finish too
        Thread.sleep(SUPPLY_VALUE_DELAY);

        // The GeneratorJob run() should have failed twice just before the final
        // transaction and succeeded a third time
        assertTrue(GeneratorJob.runCount.get() >= 2);

        // The ChildJob should only have been run once. The two orphaned jobs were
        // never run
        assertEquals(1, ChildJob.runCount.get());

        // Get all of the Pipeline objects so we can confirm the orphaned jobs are
        // really there
        final PipelineObjects allObjects = pipelineManager.queryFullPipeline(pipelineKey);
        final UUID rootJobKey = pipelineKey;
        final JobRecord rootJob = allObjects.getJobs().get(rootJobKey);
        assertNotNull(rootJob);
        final UUID graphKey = rootJob.getChildGraphKey();
        assertNotNull(graphKey);
        final int numJobs = allObjects.getJobs().size();
        assertEquals(2, numJobs);
        int numOrphanedJobs = 0;
        int numNonOrphanedJobs = 0;

        // Look through all of the JobRecords in the data store
        for (final JobRecord record : allObjects.getJobs().values()) {
            // They all have the right rooJobKey
            assertEquals(rootJobKey, record.getPipelineKey());
            if (record.getKey().equals(rootJobKey)) {
                // This one is the root job
                assertNull(record.getGraphKey());
                assertNull(record.getGeneratorJobKey());
                continue;
            }
            // If its not the root job then it was generated by the root job
            assertEquals(rootJobKey, record.getGeneratorJobKey());

            // Count the generated jobs that are orphaned and not orphaned
            if (graphKey.equals(record.getGraphKey())) {
                numNonOrphanedJobs++;
            } else {
                numOrphanedJobs++;
            }
        }

        // There should be one non-orphaned and at least two orphaned
        assertEquals(1, numNonOrphanedJobs);
        assertEquals(0, numOrphanedJobs);

        if (usePromisedValue) {
            // If we are using promised-value activation then an
            // OrphanedObjectException should have been caught at least twice.
            final int orphanedObjectExcetionCount =
                    SupplyPromisedValueRunnable.orphanedObjectExcetionCount.get();
            assertTrue("Was expecting orphanedObjectExcetionCount to be more than one, but it was "
                    + orphanedObjectExcetionCount, orphanedObjectExcetionCount >= 2);
        }

        // Now delete the whole Pipeline
        service.deletePipelineRecords(pipelineKey);

        // Check that all jobs have been deleted
        final AppEngineBackEnd backend = (AppEngineBackEnd) pipelineManager.getBackEnd();
        final AtomicInteger numJobs2 = new AtomicInteger(0);
        backend.queryAll(null, JobRecord.DATA_STORE_KIND, JobRecord.PROPERTIES, rootJobKey, struct -> {
            numJobs2.addAndGet(1);
        });
        assertEquals(0, numJobs2.get());
    }

    /**
     * This job will fail just before the final transaction the first two times it
     * is attempted, leaving an orphaned child job graph each time. It will
     * succeed the third time. The job also counts the number of times it was run.
     */
    @SuppressWarnings("serial")
    private static class GeneratorJob extends Job0<Void> {

        private static final String SHOULD_FAIL_PROPERTY =
                getFailureProperty("AppEngineBackeEnd.saveWithJobStateCheck.beforeFinalTransaction");
        private static AtomicInteger runCount = new AtomicInteger(0);
        boolean usePromise;

        GeneratorJob(final boolean usePromise) {
            this.usePromise = usePromise;
        }

        @Override
        public Value<Void> run() {
            final int count = runCount.getAndIncrement();
            if (count < 2) {
                System.setProperty(SHOULD_FAIL_PROPERTY, "true");
            }
            final Value<Integer> dummyValue;
            if (usePromise) {
                final PromisedValue<Integer> promisedValue = newPromise();
                (new Thread(new SupplyPromisedValueRunnable(service, ApiProxy.getCurrentEnvironment(),
                        getPipelineKey(), promisedValue.getKey(), runCount.get()))).start();
                LOGGER.info("Starting SupplyPromisedValueRunnable for run " + runCount);
                dummyValue = promisedValue;
            } else {
                dummyValue = immediate(0);
            }
            futureCall(new ChildJob(), dummyValue);
            return null;
        }
    }

    /**
     * This job simply counts the number of times it was run.
     */
    @SuppressWarnings("serial")
    private static class ChildJob extends Job1<Void, Integer> {
        private static AtomicInteger runCount = new AtomicInteger(0);

        @Override
        public Value<Void> run(final Integer ignored) {
            runCount.incrementAndGet();
            return null;
        }
    }

    /**
     * A {@code Runnable} for invoking the method
     * {@link PipelineService#submitPromisedValue(UUID, UUID, Object)}.
     */
    private static class SupplyPromisedValueRunnable implements Runnable {

        private static AtomicInteger orphanedObjectExcetionCount = new AtomicInteger(0);
        private final PipelineService service;
        private final UUID pipelineKey;
        private UUID promiseKey;
        private ApiProxy.Environment apiProxyEnvironment;
        private int runNum;

        SupplyPromisedValueRunnable(final PipelineService service, final ApiProxy.Environment environment, final UUID pipelineKey, final UUID promiseKey, final int runNum) {
            this.service = service;
            this.pipelineKey = pipelineKey;
            this.promiseKey = promiseKey;
            this.apiProxyEnvironment = environment;
            this.runNum = runNum;
        }

        @Override
        public void run() {
            ApiProxy.setEnvironmentForCurrentThread(apiProxyEnvironment);
            // TODO(user): Try something better than sleep to make sure
            // this happens after the processing the caller's runTask
            LOGGER.info("SupplyPromisedValueRunnable for run " + runNum + " and key " + promiseKey + " going asleep");
            try {
                Thread.sleep(SUPPLY_VALUE_DELAY);
            } catch (InterruptedException ignore) {
                // ignore - use uninterruptables
            }
            LOGGER.info("SupplyPromisedValueRunnable for run " + runNum + " and key " + promiseKey + " awake");
            try {
                service.submitPromisedValue(pipelineKey, promiseKey, 0);
            } catch (NoSuchObjectException e) {
                throw new RuntimeException(e);
            } catch (OrphanedObjectException e) {
                orphanedObjectExcetionCount.incrementAndGet();
                LOGGER.info("SupplyPromisedValueRunnable for run " + runNum + " and key " + promiseKey + " gon OrphanedObjectException");
            }
        }
    }
}
