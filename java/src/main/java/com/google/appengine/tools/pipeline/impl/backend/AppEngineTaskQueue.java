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

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueConstants;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.pipeline.Route;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.ServiceUtils;
import com.google.apphosting.api.ApiProxy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Encapsulates access to the App Engine Task Queue API
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class AppEngineTaskQueue implements PipelineTaskQueue {

    static final int MAX_TASKS_PER_ENQUEUE = QueueConstants.maxTasksPerAdd();
    private static final Logger LOGGER = Logger.getLogger(AppEngineTaskQueue.class.getName());

    private static Queue getQueue(final String queueName) {
        String queue = queueName;
        if (queue == null) {
            final Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
            queue = (String) attributes.get(TaskHandler.TASK_QUEUE_NAME_HEADER);
        }
        return queue == null ? QueueFactory.getDefaultQueue() : QueueFactory.getQueue(queue);
    }

    private static void addProperties(final TaskOptions taskOptions, final Properties properties) {
        for (final String paramName : properties.stringPropertyNames()) {
            final String paramValue = properties.getProperty(paramName);
            taskOptions.param(paramName, paramValue);
        }
    }

    @Override
    public void enqueue(final Task task) {
        LOGGER.finest("Enqueueing: " + task);
        final TaskOptions taskOptions = toTaskOptions(task);
        final Queue queue = getQueue(task.getQueueSettings().getOnQueue());
        try {
            queue.add(taskOptions);
        } catch (TaskAlreadyExistsException ignore) {
            // ignore
        }
    }

    @Override
    public void enqueue(final Collection<Task> tasks) {
        addToQueue(tasks);
    }

    //VisibleForTesting
    List<TaskHandle> addToQueue(final Collection<Task> tasks) {
        final List<TaskHandle> handles = new ArrayList<>();
        final Map<String, List<TaskOptions>> queueNameToTaskOptions = new HashMap<>();
        for (final Task task : tasks) {
            LOGGER.finest("Enqueueing: " + task);
            final String queueName = task.getQueueSettings().getOnQueue();
            final TaskOptions taskOptions = toTaskOptions(task);
            List<TaskOptions> taskOptionsList = queueNameToTaskOptions.get(queueName);
            if (taskOptionsList == null) {
                taskOptionsList = new ArrayList<>();
                queueNameToTaskOptions.put(queueName, taskOptionsList);
            }
            taskOptionsList.add(taskOptions);
        }
        for (final Map.Entry<String, List<TaskOptions>> entry : queueNameToTaskOptions.entrySet()) {
            final Queue queue = getQueue(entry.getKey());
            handles.addAll(addToQueue(queue, entry.getValue()));
        }
        return handles;
    }

    private List<TaskHandle> addToQueue(final Queue queue, final List<TaskOptions> tasks) {
        final int limit = tasks.size();
        int start = 0;
        final List<Future<List<TaskHandle>>> futures = new ArrayList<>(limit / MAX_TASKS_PER_ENQUEUE + 1);
        while (start < limit) {
            final int end = Math.min(limit, start + MAX_TASKS_PER_ENQUEUE);
            futures.add(queue.addAsync(tasks.subList(start, end)));
            start = end;
        }

        final List<TaskHandle> taskHandles = new ArrayList<>(limit);
        for (final Future<List<TaskHandle>> future : futures) {
            try {
                taskHandles.addAll(future.get());
            } catch (InterruptedException e) {
                LOGGER.throwing("AppEngineTaskQueue", "addToQueue", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException("addToQueue failed", e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TaskAlreadyExistsException) {
                    // Ignore, as that suggests all non-duplicate tasks were sent successfully
                } else {
                    throw new RuntimeException("addToQueue failed", e.getCause());
                }
            }
        }
        return taskHandles;
    }

    private TaskOptions toTaskOptions(final Task task) {
        final QueueSettings queueSettings = task.getQueueSettings();

        final TaskOptions taskOptions = TaskOptions.Builder.withUrl(TaskHandler.handleTaskUrl());
        final Route route = queueSettings.getRoute();
        if (route.getHeaders() != null) {
            taskOptions.headers(route.getHeaders());
        }
        String service = route.getService();
        String version = route.getVersion();
        if (service == null) {
            service = ServiceUtils.getCurrentService();
            version = ServiceUtils.getCurrentVersion();
        }
        if (route.getHeaders() != null && !route.getHeaders().containsKey("Host")) {
            taskOptions.header("Host", version + "." + service);
        }

        final Long delayInSeconds = queueSettings.getDelayInSeconds();
        if (null != delayInSeconds) {
            taskOptions.countdownMillis(Duration.ofSeconds(delayInSeconds).toMillis());
            queueSettings.setDelayInSeconds(null);
        }
        addProperties(taskOptions, task.toProperties());
        final String taskName = task.getName();
        if (null != taskName) {
            taskOptions.taskName(taskName);
        }
        return taskOptions;
    }
}
