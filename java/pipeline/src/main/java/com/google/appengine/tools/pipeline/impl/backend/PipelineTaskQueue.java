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

import com.google.appengine.tools.pipeline.impl.tasks.Task;

import javax.servlet.http.HttpServletRequest;
import java.io.Serializable;
import java.util.Collection;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public interface PipelineTaskQueue {
    void enqueue(Task task);

    void enqueue(Collection<Task> tasks);

    void enqueueDeferred(String queueName, Deferred deferred);

    interface Deferred extends Serializable {
        void run(HttpServletRequest request, DeferredContext context);
    }

    interface DeferredContext {
        void markForRetry();
    }

    class TaskAlreadyExistsException extends RuntimeException {
        public TaskAlreadyExistsException(final Throwable cause) {
            super(cause);
        }
    }
}