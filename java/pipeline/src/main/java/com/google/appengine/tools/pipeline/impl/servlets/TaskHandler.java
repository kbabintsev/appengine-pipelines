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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.StringUtils;

import jakarta.inject.Inject;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A ServletHelper that handles all requests from the task queue.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class TaskHandler {

    public static final String TASK_NAME_REQUEST_HEADER = "X-AppEngine-TaskName";
    public static final String TASK_RETRY_COUNT_HEADER = "X-AppEngine-TaskRetryCount";
    public static final String TASK_QUEUE_NAME_HEADER = "X-AppEngine-QueueName";
    static final String PATH_COMPONENT = "handleTask";
    private static Logger logger = Logger.getLogger(TaskHandler.class.getName());
    private final PipelineManager pipelineManager;

    @Inject
    public TaskHandler(final PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
    }

    public static String handleTaskUrl() {
        return PipelineServlet.baseUrl() + PATH_COMPONENT;
    }

    private static Task reconstructTask(final HttpServletRequest request) {
        final Properties properties = new Properties();
        final Enumeration<?> paramNames = request.getParameterNames();
        while (paramNames.hasMoreElements()) {
            final String paramName = (String) paramNames.nextElement();
            final String paramValue = request.getParameter(paramName);
            properties.setProperty(paramName, paramValue);
        }
        final String taskName = request.getHeader(TASK_NAME_REQUEST_HEADER);
        final Task task = Task.fromProperties(taskName, properties);
        task.getQueueSettings().setDelayInSeconds(null);
        final String queueName = request.getHeader(TASK_QUEUE_NAME_HEADER);
        if (queueName != null && !queueName.isEmpty()) {
            final String onQueue = task.getQueueSettings().getOnQueue();
            if (onQueue == null || onQueue.isEmpty()) {
                task.getQueueSettings().setOnQueue(queueName);
            }
//            final Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
//            attributes.put(TASK_QUEUE_NAME_HEADER, queueName);
        }
        return task;
    }

    void doPost(final HttpServletRequest req) throws ServletException {
        final Task task = reconstructTask(req);
        int retryCount;
        try {
            retryCount = req.getIntHeader(TASK_RETRY_COUNT_HEADER);
        } catch (NumberFormatException e) {
            retryCount = -1;
        }
        try {
            pipelineManager.processTask(task);
        } catch (RuntimeException e) {
            StringUtils.logRetryMessage(logger, task, retryCount, e);
            throw new ServletException(e);
        }
    }
}
