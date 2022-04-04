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

package com.google.appengine.tools.pipeline;

import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.JobSetting.BackoffFactor;
import com.google.appengine.tools.pipeline.JobSetting.BackoffSeconds;
import com.google.appengine.tools.pipeline.JobSetting.MaxAttempts;
import junit.framework.AssertionFailedError;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class RetryTest extends PipelineTest {

    public static final int ACCEPTABLE_LAG_SECONDS = 5;
    private static volatile CountDownLatch countdownLatch;

    public RetryTest() {
        final LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
        taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
        taskQueueConfig.setDisableAutoTaskExecution(false);
        taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
//        helper = new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(), taskQueueConfig,
//                new LocalModulesServiceTestConfig());
    }

//    @Override
//    public void setUp() throws Exception {
//        super.setUp();
//        helper.setUp();
//        System.setProperty(USE_SIMPLE_UUIDS_FOR_DEBUGGING, "true");
//    }
//
//    @Override
//    public void tearDown() throws Exception {
//        helper.tearDown();
//        super.tearDown();
//    }

    public void testMaxAttempts() throws Exception {
        doMaxAttemptsTest(true);
        doMaxAttemptsTest(false);
    }

    public void testLongBackoffTime() throws Exception {
        // Fail twice with a 3 second backoff factor. Wait 5 seconds. Should
        // succeed.
        runJob(3, 2, 5, false);

        // Fail 3 times with a 3 second backoff factor. Wait 10 seconds. Should fail
        // because 3 + 9 = 12 > 10
        try {
            runJob(3, 3, 10, false);
            fail("Excepted exception");
        } catch (AssertionFailedError ignore) {
            // expected;
        }

        // Fail 3 times with a 3 second backoff factor. Wait 15 seconds. Should
        // succeed
        // because 3 + 9 = 12 < 15
        runJob(3, 3, 15, false);
    }

    private void doMaxAttemptsTest(final boolean succeedTheLastTime) throws Exception {
        final UUID pipelineId = runJob(1, 4, 10, succeedTheLastTime);
        // Wait for framework to save Job information
        Thread.sleep(1000L + ACCEPTABLE_LAG_SECONDS * 1000);
        final JobInfo jobInfo = service.getJobInfo(pipelineId, pipelineId);
        final JobInfo.State expectedState = succeedTheLastTime
                ? JobInfo.State.COMPLETED_SUCCESSFULLY
                : JobInfo.State.STOPPED_BY_ERROR;
        assertEquals(expectedState, jobInfo.getJobState());
    }

    private UUID runJob(final int backoffFactor, final int maxAttempts, final int awaitSeconds,
                        final boolean succeedTheLastTime) throws Exception {
        countdownLatch = new CountDownLatch(maxAttempts);

        final UUID pipelineId = service.startNewPipeline(
                new InvokesFailureJob(succeedTheLastTime, maxAttempts, backoffFactor));
        assertTrue(countdownLatch.await(awaitSeconds + ACCEPTABLE_LAG_SECONDS, TimeUnit.SECONDS));
        return pipelineId;
    }

    /**
     * A job that invokes {@link FailureJob}.
     */
    @SuppressWarnings("serial")
    public static class InvokesFailureJob extends Job0<Void> {
        int maxAttempts;
        int backoffFactor;
        private boolean succeedTheLastTime;

        public InvokesFailureJob(final boolean succeedTheLastTime, final int maxAttempts, final int backoffFactor) {
            this.succeedTheLastTime = succeedTheLastTime;
            this.maxAttempts = maxAttempts;
            this.backoffFactor = backoffFactor;
        }

        @Override
        public Value<Void> run() {
            final JobSetting[] jobSettings =
                    new JobSetting[]{new MaxAttempts(maxAttempts), new BackoffSeconds(1),
                            new BackoffFactor(backoffFactor)};
            return futureCall(new FailureJob(succeedTheLastTime), jobSettings);
        }
    }

    /**
     * A job that fails every time except possibly the last time.
     */
    @SuppressWarnings("serial")
    public static class FailureJob extends Job0<Void> {
        private boolean succeedTheLastTime;

        public FailureJob(final boolean succeedTheLastTime) {
            this.succeedTheLastTime = succeedTheLastTime;
        }

        @Override
        public Value<Void> run() {
            countdownLatch.countDown();
            if (countdownLatch.getCount() == 0 && succeedTheLastTime) {
                return null;
            }
            throw new RuntimeException("Hello");
        }
    }
}
