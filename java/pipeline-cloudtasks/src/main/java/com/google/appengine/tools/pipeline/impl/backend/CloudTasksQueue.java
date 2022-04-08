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

import com.cloudaware.deferred.DeferredTaskContext;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudtasks.v2.CloudTasks;
import com.google.api.services.cloudtasks.v2.CloudTasksScopes;
import com.google.api.services.cloudtasks.v2.model.AppEngineHttpRequest;
import com.google.api.services.cloudtasks.v2.model.AppEngineRouting;
import com.google.api.services.cloudtasks.v2.model.CreateTaskRequest;
import com.google.appengine.tools.pipeline.Route;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.common.collect.ImmutableList;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class CloudTasksQueue implements PipelineTaskQueue {

    static final FailsafeExecutor<Object> RETRY_HELPER = Failsafe.with(
            RetryPolicy.builder()
                    .withMaxAttempts(6)
                    .withBackoff(1, 4, ChronoUnit.SECONDS)
                    .abortOn(GoogleJsonResponseException.class)
                    .handle(SocketTimeoutException.class)
                    .handle(IOException.class)
                    .build()
    );
    private static final String CLOUDTASKS_API_ROOT_URL_PROPERTY = "cloudtasks.api.root.url";
    private static final String CLOUDTASKS_GZIP_DISABLE = "cloudtasks.gzip.disable";
    private static final String CLOUDTASKS_API_KEY_PROPERTY = "cloudtasks.api.key";
    private static final String CLOUDTASKS_API_DEFAULT_PARENT = "cloudtasks.api.default.parent";
    private static final int HTTP_409 = 409;
    private static final int DEFERRED_DELAY_SECONDS = 10;
    private static final Logger LOGGER = Logger.getLogger(CloudTasksQueue.class.getName());
    private final String apiKey;
    private final CloudTasks cloudTask;
    private final boolean gzipDisable;

    public CloudTasksQueue() {
        final String rootUrl = System.getProperty(CLOUDTASKS_API_ROOT_URL_PROPERTY);
        this.apiKey = System.getProperty(CLOUDTASKS_API_KEY_PROPERTY);
        this.gzipDisable = "true".equalsIgnoreCase(System.getProperty(CLOUDTASKS_GZIP_DISABLE));
        try {
            HttpRequestInitializer credential;
            try {
                credential = GoogleCredential.getApplicationDefault().createScoped(CloudTasksScopes.all());
            } catch (IOException e) {
                LOGGER.info("Fallback to HttpRequestInitializer, cause cannot create Application default credentials: " + e.getMessage());
                credential = new HttpRequestInitializer() {
                    @Override
                    public void initialize(final HttpRequest request) throws IOException {
                    }
                };
            }
            final CloudTasks.Builder builder = new CloudTasks.Builder(
                    GoogleNetHttpTransport.newTrustedTransport(),
                    JacksonFactory.getDefaultInstance(),
                    credential
            ).setApplicationName("appengine-pipeline");
            if (rootUrl != null) {
                builder.setRootUrl(rootUrl);
            }
            cloudTask = builder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CloudTasksQueue(final CloudTasks cloudTask) {
        this(cloudTask, null);
    }

    public CloudTasksQueue(final CloudTasks cloudTask, final String apiKey) {
        this.apiKey = apiKey;
        this.cloudTask = cloudTask;
        this.gzipDisable = false;
    }

    private static String getCurrentService() {
        final String service = System.getenv("GAE_SERVICE");
        return service == null ? System.getProperty("GAE_SERVICE") : service;
    }

    private static String getCurrentVersion() {
        final String version = System.getenv("GAE_VERSION");
        return version == null ? System.getProperty("GAE_VERSION") : version;
    }

    @Override
    public void enqueue(final Task task) {
        addToQueue(ImmutableList.of(task));
    }

    @Override
    public void enqueue(final Collection<Task> tasks) {
        addToQueue(tasks);
    }

    @Override
    public void enqueueDeferred(final String queueName, final Deferred deferred) {
        final com.google.api.services.cloudtasks.v2.model.Task task = new com.google.api.services.cloudtasks.v2.model.Task();
        task.setAppEngineHttpRequest(new AppEngineHttpRequest());
        task.setScheduleTime(getScheduleTime(DEFERRED_DELAY_SECONDS));
        task.getAppEngineHttpRequest()
                .setAppEngineRouting(
                        new AppEngineRouting()
                                .setService(getCurrentService())
                                .setVersion(getCurrentVersion())
                );
        DeferredTaskContext.enqueueDeferred(new CloudTasksDeferredTask(deferred), task, queueName);
    }

    //VisibleForTesting
    void addToQueue(final Collection<Task> tasks) {
        for (final Task task : tasks) {
            LOGGER.finest("Enqueueing: " + task);
            final String queueName = Optional.ofNullable(task.getQueueSettings().getOnQueue()).orElse("default");
            try {
                RETRY_HELPER.get(() -> cloudTask
                        .projects()
                        .locations()
                        .queues()
                        .tasks()
                        .create(
                                getQueueName(queueName),
                                new CreateTaskRequest().setTask(toTaskOptions(task))
                        )
                        .setDisableGZipContent(this.gzipDisable)
                        .setKey(this.apiKey)
                        .execute()
                );
            } catch (FailsafeException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof GoogleJsonResponseException && ((GoogleJsonResponseException) cause).getStatusCode() == HTTP_409) {
                    //ignore TaskAlreadyExist
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private String getQueueName(final String queue) {
        final String queueName;
        if (queue == null || !queue.startsWith("projects/")) {
            queueName = System.getProperty(CLOUDTASKS_API_DEFAULT_PARENT) + "/queues/" + (queue == null ? "default" : queue);
        } else {
            queueName = queue;
        }
        return queueName;
    }

    private com.google.api.services.cloudtasks.v2.model.Task toTaskOptions(final Task task) {
        final QueueSettings queueSettings = task.getQueueSettings();
        final Properties props = task.toProperties();
        String relativeUrl = TaskHandler.handleTaskUrl();
        if (!props.stringPropertyNames().isEmpty()) {
            relativeUrl += "?" + props.stringPropertyNames().stream().map(o -> o + "=" + props.getProperty(o)).collect(Collectors.joining("&"));
        }

        final Route route = queueSettings.getRoute();
        final com.google.api.services.cloudtasks.v2.model.Task taskOptions = new com.google.api.services.cloudtasks.v2.model.Task();
        taskOptions.setAppEngineHttpRequest(
                new AppEngineHttpRequest()
                        .setHeaders(route.getHeaders())
                        .setRelativeUri(relativeUrl)
        );

        AppEngineRouting appEngineRouting = taskOptions.getAppEngineHttpRequest().getAppEngineRouting();
        if (appEngineRouting == null) {
            appEngineRouting = new AppEngineRouting();
            taskOptions.getAppEngineHttpRequest().setAppEngineRouting(appEngineRouting);
        }
        String onService = route.getService();
        if (onService == null) {
            onService = getCurrentService();
        }
        appEngineRouting.setService(onService);
        String onVersion = route.getVersion();
        if (onVersion == null) {
            onVersion = getCurrentVersion();
        }
        appEngineRouting.setVersion(onVersion);
        final String onInstance = route.getInstance();
        if (onInstance != null) {
            appEngineRouting.setInstance(onInstance);
        }

        final Long delayInSeconds = queueSettings.getDelayInSeconds();
        if (null != delayInSeconds) {
            taskOptions.setScheduleTime(getScheduleTime(delayInSeconds.intValue()));
        }

        final String taskName = task.getName();
        if (null != taskName && null != task.getQueueSettings().getOnQueue()) {
            final String fullName = getQueueName(task.getQueueSettings().getOnQueue()) + "/tasks/" + taskName;
            taskOptions.setName(fullName);
        }
        return taskOptions;
    }

    /**
     * Add seconds to Now() and serialize it to ISO 8601
     *
     * @param seconds amount of seconds to add to now()
     * @return date in ISO 8601
     */
    private String getScheduleTime(final int seconds) {
        return DeferredTaskContext.getScheduleTime(System.currentTimeMillis() + Duration.ofSeconds(seconds).toMillis());
    }

}
