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
import com.google.appengine.tools.pipeline.impl.util.StringUtils;
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * A Task that represent a set of other tasks.
 * <p>
 * The purpose of this class is to get around two limitations in App Engine:
 * <ol>
 * <li>Only a small number (currently 5) of task queue tasks is permitted to be
 * part of of a data store transaction.
 * <li>Named task queue tasks may not be part of a data store transaction.
 * </ol>
 * <p>
 * This task is used as part of a strategy to get around those limitations:
 * Instead of enqueueing a set of tasks, a single {@code FanoutTask} may be
 * enqueued that, when handled, will cause a collection of other tasks to be
 * enqueued.
 * <p>
 * Given a Collection of Tasks, the static method
 * {@link #encodeTasks(Collection)} may be used to encode the Collection into a
 * byte array which may be persisted to the data store. An instance of this
 * class may contain the data store key of the entity containing the byte array.
 * This single task may be enqueued as part of a data store transaction. Later,
 * when the task is handled, the data store key may be retrieved from an
 * instance of this class and the byte array retrieved from the data store. Then
 * the static method {@link #decodeTasks(byte[])} may be used to reconstitute
 * the original Collection of Tasks. Finally each of the tasks in the collection
 * may be enqueued non-transactionally.
 *
 * @author rudominer@google.com (Mitch Rudominer)
 * @see com.google.appengine.tools.pipeline.impl.backend.PipelineBackEnd#handleFanoutTask
 */
public final class FanoutTask extends Task {

    private static final String KEY_VALUE_SEPARATOR = "::";
    private static final String PROPERTY_SEPARATOR = ",,";
    private static final String TASK_NAME_DELIMITER = "--";
    private static final String TASK_SEPARATOR = ";;";
    private static final String RECORD_KEY_PROPERTY = "recordKey";

    private final UUID recordKey;

    /**
     * Construct a new FanoutTask that contains the given data store Key. This
     * constructor is used to construct an instance to be enqueued.
     */
    public FanoutTask(final UUID recordKey, final QueueSettings queueSettings) {
        super(Type.FAN_OUT, null, queueSettings.clone());
        this.recordKey = recordKey;
    }

    /**
     * Construct a new FanoutTask from the given Properties. This constructor
     * is used to construct an instance that is being handled.
     */
    public FanoutTask(final Type type, final String taskName, final Properties properties) {
        super(type, taskName, properties);
        this.recordKey = UUID.fromString(properties.getProperty(RECORD_KEY_PROPERTY));
    }

    public static byte[] encodeTasks(final Collection<? extends Task> taskList) {
        if (taskList.isEmpty()) {
            return new byte[0];
        }
        final StringBuilder builder = new StringBuilder(1024);
        for (final Task task : taskList) {
            encodeTask(builder, task);
            builder.append(TASK_SEPARATOR);
        }
        builder.setLength(builder.length() - TASK_SEPARATOR.length());
        return builder.toString().getBytes(StringUtils.UTF_8);
    }

    private static void encodeTask(final StringBuilder builder, final Task task) {
        final String taskName = UuidGenerator.nextUuid().toString();
        builder.append(taskName);
        builder.append(TASK_NAME_DELIMITER);
        final Properties taskProps = task.toProperties();
        if (!taskProps.isEmpty()) {
            for (final String propName : taskProps.stringPropertyNames()) {
                final String value = taskProps.getProperty(propName);
                builder.append(propName).append(KEY_VALUE_SEPARATOR).append(value);
                builder.append(PROPERTY_SEPARATOR);
            }
            builder.setLength(builder.length() - PROPERTY_SEPARATOR.length());
        }
    }

    public static List<Task> decodeTasks(final byte[] encodedBytes) {
        final String encodedListOfTasks = new String(encodedBytes, StringUtils.UTF_8);
        final String[] encodedTaskArray = encodedListOfTasks.split(TASK_SEPARATOR);
        final List<Task> listOfTasks = new ArrayList<>(encodedTaskArray.length);
        for (final String encodedTask : encodedTaskArray) {
            final String[] nameAndProperties = encodedTask.split(TASK_NAME_DELIMITER);
            final String taskName = nameAndProperties[0];
            final String encodedProperties = nameAndProperties[1];
            final String[] encodedPropertyArray = encodedProperties.split(PROPERTY_SEPARATOR);
            final Properties taskProperties = new Properties();
            for (final String encodedProperty : encodedPropertyArray) {
                final String[] keyValuePair = encodedProperty.split(KEY_VALUE_SEPARATOR);
                final String key = keyValuePair[0];
                final String value = keyValuePair[1];
                taskProperties.setProperty(key, value);
            }
            final Task task = Task.fromProperties(taskName, taskProperties);
            listOfTasks.add(task);
        }
        return listOfTasks;
    }

    @Override
    protected void addProperties(final Properties properties) {
        properties.setProperty(RECORD_KEY_PROPERTY, recordKey.toString());
    }

    public UUID getRecordKey() {
        return recordKey;
    }

    @Override
    public String propertiesAsString() {
        return "key=" + recordKey;
    }
}