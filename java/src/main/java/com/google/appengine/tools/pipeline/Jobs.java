// Copyright 2014 Google Inc.
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

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import javax.servlet.http.HttpServletRequest;
import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A collection of common jobs and utilities.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class Jobs {

    private Jobs() {
        // A utility class
    }

    public static JobSetting.WaitForSetting[] createWaitForSettingArray(final Value<?>... values) {
        final JobSetting.WaitForSetting[] settings = new JobSetting.WaitForSetting[values.length];
        int i = 0;
        for (final Value<?> value : values) {
            settings[i++] = Job.waitFor(value);
        }
        return settings;
    }

    public static <T> Value<T> waitForAll(final Job<?> caller, final Value<T> value, final Value<?>... values) {
        return caller.futureCall(new WaitForAllJob<T>(), value, createWaitForSettingArray(values));
    }

    public static <T> Value<T> waitForAll(final Job<?> caller, final T value, final Value<?>... values) {
        return caller.futureCall(new WaitForAllJob<T>(), Job.immediate(value),
                createWaitForSettingArray(values));
    }

    public static <T> Value<T> waitForAllAndDelete(
            final Job<?> caller, final Value<T> value, final Value<?>... values) {
        return caller.futureCall(
                new DeletePipelineJob<T>(caller.getPipelineKey()),
                value, createWaitForSettingArray(values));
    }

    public static <T> Value<T> waitForAllAndDelete(final Job<?> caller, final T value, final Value<?>... values) {
        return caller.futureCall(new DeletePipelineJob<T>(caller.getPipelineKey()),
                Job.immediate(value), createWaitForSettingArray(values));
    }

    /**
     * An helper job to transform a {@link Value} result.
     *
     * @param <F> input value.
     * @param <T> transformed value.
     */
    public static class Transform<F, T> extends Job1<T, F> {

        private static final long serialVersionUID = 1280795955105207728L;
        private Function<F, T> function;

        public Transform(final Function<F, T> function) {
            Preconditions.checkArgument(function instanceof Serializable, "Function not serializable");
            this.function = function;
        }

        @Override
        public Value<T> run(final F from) throws Exception {
            return immediate(function.apply(from));
        }
    }

    private static class WaitForAllJob<T> extends Job1<T, T> {

        private static final long serialVersionUID = 3151677893523195265L;

        @Override
        public Value<T> run(final T value) {
            return immediate(value);
        }
    }

    private static class DeletePipelineJob<T> extends Job1<T, T> {

        private static final long serialVersionUID = -5440838671291502355L;
        private static final Logger LOGGER = Logger.getLogger(DeletePipelineJob.class.getName());
        private static final int COUNTDOWN_MILLIS = 10000;
        private static final int MAX_BACKOFF_SECONDS = 20;
        private static final int MIN_BACKOFF_SECONDS = 2;
        private final UUID key;

        DeletePipelineJob(final UUID rootJobKey) {
            this.key = rootJobKey;
        }

        @Override
        public Value<T> run(final T value) {
            DeferredTask deleteRecordsTask = new DeferredTask() {
                private static final long serialVersionUID = -7510918963650055768L;

                @Override
                public void run() {
                    final PipelineService service = PipelineServiceFactory.newPipelineService();
                    try {
                        service.deletePipelineRecords(key);
                        LOGGER.info("Deleted pipeline: " + key);
                    } catch (IllegalStateException e) {
                        LOGGER.info("Failed to delete pipeline: " + key);
                        final HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
                        if (request != null) {
                            final int attempts = request.getIntHeader("X-AppEngine-TaskExecutionCount");
                            if (attempts <= 5) {
                                LOGGER.info("Request to retry deferred task #" + attempts);
                                DeferredTaskContext.markForRetry();
                                return;
                            }
                        }
                        try {
                            service.deletePipelineRecords(key, true, false);
                            LOGGER.info("Force deleted pipeline: " + key);
                        } catch (Exception ex) {
                            LOGGER.log(Level.WARNING, "Failed to force delete pipeline: " + key, ex);
                        }
                    } catch (NoSuchObjectException ignore) {
                        // Already done
                    }
                }
            };
            final String queueName = Optional.ofNullable(getOnQueue()).orElse("default");
            final Queue queue = QueueFactory.getQueue(queueName);
            queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(COUNTDOWN_MILLIS)
                    .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(MIN_BACKOFF_SECONDS).maxBackoffSeconds(MAX_BACKOFF_SECONDS)));
            return immediate(value);
        }
    }
}
