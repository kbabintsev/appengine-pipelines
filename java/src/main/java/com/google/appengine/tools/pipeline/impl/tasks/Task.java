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

package com.google.appengine.tools.pipeline.impl.tasks;

import com.google.appengine.tools.pipeline.impl.QueueSettings;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;

/**
 * An App Engine Task Queue Task. This is the abstract base class for all
 * Pipeline task types.
 * <p>
 * This class represents both a task to be enqueued and a task being handled.
 * <p>
 * When enqueueing a task, construct a concrete subclass with the appropriate
 * data, and then add the task to an
 * {@link com.google.appengine.tools.pipeline.impl.backend.UpdateSpec} and
 * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#save save}.
 * Alternatively the task may be enqueued directly using
 * {@link com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#enqueue(Task)}.
 * <p>
 * When handling a task, construct a {@link Properties} object containing the
 * relevant parameters from the request, its name and then invoke
 * {@link #fromProperties(String, Properties)}.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 */
public abstract class Task {

    protected static final String TASK_TYPE_PARAMETER = "taskType";
    private final Type type;
    private final String taskName;
    private final QueueSettings queueSettings;

    /**
     * This constructor is used on the sending side. That is, it is used to
     * construct a task to be enqueued.
     */
    protected Task(final Type type, final String taskName, final QueueSettings queueSettings) {
        if (type == null) {
            throw new IllegalArgumentException("type must not be null");
        }
        if (queueSettings == null) {
            throw new IllegalArgumentException("queueSettings must not be null");
        }
        this.type = type;
        this.taskName = taskName;
        this.queueSettings = queueSettings;
    }

    protected Task(final Type type, final String taskName, final Properties properties) {
        this(type, taskName, new QueueSettings());
        for (final TaskProperty taskProperty : TaskProperty.ALL) {
            taskProperty.applyFrom(this, properties);
        }
    }

    /**
     * Construct a task from {@code Properties}. This method is used on the
     * receiving side. That is, it is used to construct a {@code Task} from an
     * HttpRequest sent from the App Engine task queue. {@code properties} must
     * contain a property named {@link #TASK_TYPE_PARAMETER}. In addition it must
     * contain the properties specified by the concrete subclass of this class
     * corresponding to the task type.
     */
    public static Task fromProperties(final String taskName, final Properties properties) {
        final String taskTypeString = properties.getProperty(TASK_TYPE_PARAMETER);
        if (null == taskTypeString) {
            throw new IllegalArgumentException(TASK_TYPE_PARAMETER + " property is missing: "
                    + properties.toString());
        }
        final Type type = Type.valueOf(taskTypeString);
        return type.createInstance(taskName, properties);
    }

    public final Type getType() {
        return type;
    }

    /**
     * Returns the name of the task (can be overriden by subclasses)
     *
     * @return task name
     */
    public String getName() {
        return taskName;
    }

    public final QueueSettings getQueueSettings() {
        return queueSettings;
    }

    public final Properties toProperties() {
        final Properties properties = new Properties();
        properties.setProperty(TASK_TYPE_PARAMETER, type.toString());
        for (final TaskProperty taskProperty : TaskProperty.ALL) {
            taskProperty.addTo(this, properties);
        }
        addProperties(properties);
        return properties;
    }

    @Override
    public String toString() {
        String value = getType() + "_TASK[name=" + getName() + ", queueSettings=" + getQueueSettings();
        final String extraProperties = propertiesAsString();
        if (extraProperties != null && !extraProperties.isEmpty()) {
            value += ", " + extraProperties;
        }
        return value + "]";
    }

    protected abstract String propertiesAsString();

    protected abstract void addProperties(Properties properties);

    private enum TaskProperty {

        ON_BACKEND {
            @Override
            void setProperty(final Task task, final String value) {
                task.getQueueSettings().setOnBackend(value);
            }

            @Override
            String getProperty(final Task task) {
                return task.getQueueSettings().getOnBackend();
            }
        },
        ON_MODULE {
            @Override
            void setProperty(final Task task, final String value) {
                task.getQueueSettings().setOnModule(value);
            }

            @Override
            String getProperty(final Task task) {
                return task.getQueueSettings().getOnModule();
            }
        },
        MODULE_VERSION {
            @Override
            void setProperty(final Task task, final String value) {
                task.getQueueSettings().setModuleVersion(value);
            }

            @Override
            String getProperty(final Task task) {
                return task.getQueueSettings().getModuleVersion();
            }
        },
        ON_QUEUE {
            @Override
            void setProperty(final Task task, final String value) {
                task.getQueueSettings().setOnQueue(value);
            }

            @Override
            String getProperty(final Task task) {
                return task.getQueueSettings().getOnQueue();
            }
        },
        DELAY {
            @Override
            void setProperty(final Task task, final String value) {
                if (value != null) {
                    task.getQueueSettings().setDelayInSeconds(Long.parseLong(value));
                }
            }

            @Override
            String getProperty(final Task task) {
                final Long delay = task.getQueueSettings().getDelayInSeconds();
                return delay == null ? null : delay.toString();
            }
        };

        static final Set<TaskProperty> ALL = EnumSet.allOf(TaskProperty.class);

        abstract void setProperty(Task task, String value);

        abstract String getProperty(Task task);

        void applyFrom(final Task task, final Properties properties) {
            final String value = properties.getProperty(name());
            if (value != null) {
                setProperty(task, value);
            }
        }

        void addTo(final Task task, final Properties properties) {
            final String value = getProperty(task);
            if (value != null) {
                properties.setProperty(name(), value);
            }
        }
    }

    /**
     * The type of task. The Pipeline framework uses several types
     */
    public enum Type {

        HANDLE_SLOT_FILLED(HandleSlotFilledTask.class),
        RUN_JOB(RunJobTask.class),
        HANDLE_CHILD_EXCEPTION(HandleChildExceptionTask.class),
        CANCEL_JOB(CancelJobTask.class),
        FINALIZE_JOB(FinalizeJobTask.class),
        FAN_OUT(FanoutTask.class),
        DELETE_PIPELINE(DeletePipelineTask.class),
        DELAYED_SLOT_FILL(DelayedSlotFillTask.class),
        // TODO(user): Remove in the future. Left for backward compatibility with 0.3
        FILL_SLOT_HANDLE_SLOT_FILLED(DelayedSlotFillTask.class);

        private final Constructor<? extends Task> taskConstructor;

        Type(final Class<? extends Task> taskClass) {
            try {
                taskConstructor = taskClass.getDeclaredConstructor(
                        getClass(), String.class, Properties.class);
                taskConstructor.setAccessible(true);
            } catch (NoSuchMethodException | SecurityException e) {
                throw new RuntimeException("Invalid Task class " + taskClass, e);
            }
        }

        public Task createInstance(final String aTaskName, final Properties properties) {
            try {
                return taskConstructor.newInstance(this, aTaskName, properties);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new RuntimeException("Unexpected exception while creating new instance for "
                        + taskConstructor.getDeclaringClass(), e);
            }
        }
    }
}
