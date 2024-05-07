package com.google.appengine.tools.pipeline;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class DeferredUsingJobsTest extends PipelineTest {

    private static final int MAX_ATTEMPTS = 10;
    private static final int AWAIT_SECNDS = 20;
    private static final int ACCEPTABLE_LAG_SECONDS = 5;
    private static final int SUCCESS_RESULT = 31337;
    private static volatile CountDownLatch countdownLatch;
    private static volatile UUID deferredJobKey;

    public void testFinalSuccess() throws Exception {
        countdownLatch = new CountDownLatch(MAX_ATTEMPTS);
        final UUID pipelineId = service.startNewPipeline(new InvokesDeferredJob(true, MAX_ATTEMPTS));
        assertTrue(countdownLatch.await(AWAIT_SECNDS + ACCEPTABLE_LAG_SECONDS, TimeUnit.SECONDS));
        final JobInfo jobInfo = waitUntilJobComplete(pipelineId);
        assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
        assertEquals(SUCCESS_RESULT, jobInfo.getOutput());
        final JobInfo jobInfo1 = service.getJobInfo(pipelineId, deferredJobKey);
    }

    public void testFinalFailure() throws Exception {
        countdownLatch = new CountDownLatch(MAX_ATTEMPTS);
        final UUID pipelineId = service.startNewPipeline(new InvokesDeferredJob(false, MAX_ATTEMPTS));
        assertTrue(countdownLatch.await(AWAIT_SECNDS + ACCEPTABLE_LAG_SECONDS, TimeUnit.SECONDS));
        final JobInfo jobInfo = waitUntilJobComplete(pipelineId);
        assertEquals(JobInfo.State.STOPPED_BY_ERROR, jobInfo.getJobState());
        assertEquals(null, jobInfo.getOutput());
        final JobInfo jobInfo1 = service.getJobInfo(pipelineId, deferredJobKey);
    }

    public void testDeferredWithEceptionHandler() throws Exception {
        countdownLatch = new CountDownLatch(MAX_ATTEMPTS);
        final UUID pipelineId = service.startNewPipeline(new InvokesDeferredJob2(false, MAX_ATTEMPTS));
        assertTrue(countdownLatch.await(AWAIT_SECNDS + ACCEPTABLE_LAG_SECONDS, TimeUnit.SECONDS));
        final JobInfo jobInfo = waitUntilJobComplete(pipelineId);
        assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, jobInfo.getJobState());
        assertEquals(-1, jobInfo.getOutput());
        final JobInfo jobInfo1 = service.getJobInfo(pipelineId, deferredJobKey);
    }

    /**
     * A job that invokes {@link DeferredJob}.
     */
    @SuppressWarnings("serial")
    public static class InvokesDeferredJob extends Job0<Integer> {
        int maxAttempts;
        private boolean succeedTheLastTime;

        public InvokesDeferredJob(final boolean succeedTheLastTime, final int maxAttempts) {
            this.succeedTheLastTime = succeedTheLastTime;
            this.maxAttempts = maxAttempts;
        }

        @Override
        public Value<Integer> run() {
            final JobSetting[] jobSettings = new JobSetting[]{new JobSetting.MaxAttempts(maxAttempts), new JobSetting.BackoffSeconds(1), new JobSetting.BackoffFactor(1)};
            return futureCall(new DeferredJob(succeedTheLastTime), jobSettings);
        }
    }

    /**
     * A job that fails every time except possibly the last time.
     */
    @SuppressWarnings("serial")
    public static class DeferredJob extends Job0<Integer> {
        private boolean succeedTheLastTime;

        public DeferredJob(final boolean succeedTheLastTime) {
            this.succeedTheLastTime = succeedTheLastTime;
        }

        @Override
        public Value<Integer> run() {
            deferredJobKey = getJobKey();
            countdownLatch.countDown();
            if (countdownLatch.getCount() == 0 && succeedTheLastTime) {
                return immediate(SUCCESS_RESULT);
            }
            throw new Retry();
        }
    }

    /**
     * A job that invokes {@link DeferredJob}.
     */
    @SuppressWarnings("serial")
    public static class InvokesDeferredJob2 extends Job0<Integer> {
        int maxAttempts;
        private boolean succeedTheLastTime;

        public InvokesDeferredJob2(final boolean succeedTheLastTime, final int maxAttempts) {
            this.succeedTheLastTime = succeedTheLastTime;
            this.maxAttempts = maxAttempts;
        }

        @Override
        public Value<Integer> run() {
            final JobSetting[] jobSettings = new JobSetting[]{new JobSetting.MaxAttempts(maxAttempts), new JobSetting.BackoffSeconds(1), new JobSetting.BackoffFactor(1)};
            return futureCall(new DeferredJob2(succeedTheLastTime), jobSettings);
        }
    }

    /**
     * A job that fails every time except possibly the last time.
     */
    @SuppressWarnings("serial")
    public static final class DeferredJob2 extends Job0<Integer> {
        private boolean succeedTheLastTime;

        public DeferredJob2(final boolean succeedTheLastTime) {
            this.succeedTheLastTime = succeedTheLastTime;
        }

        @Override
        public Value<Integer> run() {
            deferredJobKey = getJobKey();
            countdownLatch.countDown();
            if (countdownLatch.getCount() == 0 && succeedTheLastTime) {
                return immediate(SUCCESS_RESULT);
            }
            throw new Retry();
        }

        public Value<Integer> handleException(final Exception e) {
            assertNotNull(e);
            return immediate(-1);
        }
    }
}
