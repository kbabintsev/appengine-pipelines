// Copyright 2013 Google Inc.
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
import com.google.appengine.tools.pipeline.impl.model.Slot;

import java.util.Properties;
import java.util.UUID;

/**
 * A subclass of {@link ObjRefTask} used to implement delayed value.
 *
 * @author maximf@google.com (Maxim Fateev)
 * @see com.google.appengine.tools.pipeline.Job#newDelayedValue(long)
 */
public final class DelayedSlotFillTask extends ObjRefTask {

    private static final String ROOT_JOB_KEY_PARAM = "rootJobKey";

    private final UUID rootJobKey;

    /**
     * This constructor is used on the sending side. That is, it is used to
     * construct a {@code HandleSlotFilledTask}to be enqueued.
     * <p>
     *
     * @param slot          The Slot whose filling is to be handled
     * @param delay         The delay in seconds before task gets executed
     * @param rootJobKey    The key of the root job of the pipeline
     * @param queueSettings The queue settings
     */
    public DelayedSlotFillTask(final Slot slot, final long delay, final UUID rootJobKey, final QueueSettings queueSettings) {
        super(Type.DELAYED_SLOT_FILL, "delayedSlotFillTask", slot.getKey(), queueSettings);
        getQueueSettings().setDelayInSeconds(delay);
        this.rootJobKey = rootJobKey;
    }

    protected DelayedSlotFillTask(final Type type, final String taskName, final Properties properties) {
        super(type, taskName, properties);
        rootJobKey = UUID.fromString(properties.getProperty(ROOT_JOB_KEY_PARAM));
    }

    @Override
    protected void addProperties(final Properties properties) {
        super.addProperties(properties);
        properties.setProperty(ROOT_JOB_KEY_PARAM, rootJobKey.toString());
    }

    @Override
    public String propertiesAsString() {
        return super.propertiesAsString() + ", rootJobKey=" + rootJobKey;
    }

    public UUID getSlotKey() {
        return getKey();
    }

    public UUID getRootJobKey() {
        return rootJobKey;
    }
}