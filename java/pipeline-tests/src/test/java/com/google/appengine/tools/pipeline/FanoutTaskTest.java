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

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.model.FanoutTaskRecord;
import com.google.appengine.tools.pipeline.impl.tasks.FanoutTask;
import com.google.appengine.tools.pipeline.impl.tasks.FinalizeJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.HandleSlotFilledTask;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.common.collect.ImmutableList;
import junit.framework.TestCase;

import java.util.List;
import java.util.UUID;

import static com.google.appengine.tools.pipeline.impl.util.UuidGenerator.USE_SIMPLE_UUIDS_FOR_DEBUGGING;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public class FanoutTaskTest extends TestCase {

    byte[] encodedBytes;
    private LocalServiceTestHelper helper =
            new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());
    private List<? extends Task> listOfTasks;
    private QueueSettings queueSettings1 = new QueueSettings();
    private QueueSettings queueSettings2 = new QueueSettings().setOnQueue("queue1");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        helper.setUp();
        System.setProperty(USE_SIMPLE_UUIDS_FOR_DEBUGGING, "true");
        UUID key = UUID.fromString("00000000-0000-0000-0001-000000000001");
        final RunJobTask runJobTask = new RunJobTask(key, key, queueSettings1);
        key = UUID.fromString("00000000-0000-0000-0001-000000000002");
        final RunJobTask runJobTask2 = new RunJobTask(key, key, queueSettings2);
        key = UUID.fromString("00000000-0000-0000-0001-000000000003");
        final FinalizeJobTask finalizeJobTask = new FinalizeJobTask(key, key, queueSettings1);
        key = UUID.fromString("00000000-0000-0000-0002-000000000001");
        final HandleSlotFilledTask hsfTask = new HandleSlotFilledTask(key, key, queueSettings2);
        listOfTasks = ImmutableList.of(runJobTask, runJobTask2, finalizeJobTask, hsfTask);
        encodedBytes = FanoutTask.encodeTasks(listOfTasks);
    }

    @Override
    public void tearDown() throws Exception {
        helper.tearDown();
        super.tearDown();
    }

    /**
     * Tests the methods {@link FanoutTask#encodeTasks(java.util.Collection)} and
     * {@link FanoutTask#decodeTasks(byte[])}
     */
    public void testEncodeDecode() throws Exception {
        checkBytes(encodedBytes);
    }

    /**
     * Tests conversion of {@link FanoutTaskRecord} to and from an Entity
     */
    public void testFanoutTaskRecord() throws Exception {
        final UUID rootJobKey = UUID.fromString("00000000-0000-0000-0001-000000000001");
        final FanoutTaskRecord record = new FanoutTaskRecord(rootJobKey, encodedBytes);
//    Entity entity = record.toEntity();
////     reconstitute entity
//    record = new FanoutTaskRecord(entity);
        checkBytes(record.getPayload());
    }

    private void checkBytes(final byte[] bytes) {
        final List<Task> reconstituted = FanoutTask.decodeTasks(bytes);
        assertEquals(listOfTasks.size(), reconstituted.size());
        for (int i = 0; i < listOfTasks.size(); i++) {
            final Task expected = listOfTasks.get(i);
            final Task actual = reconstituted.get(i);
            assertEquals(i, expected, actual);
        }
    }

    private void assertEquals(final int i, final Task expected, final Task actual) {
        assertEquals("i=" + i, expected.getType(), actual.getType());
        assertEquals("i=" + i, expected.toProperties(), actual.toProperties());
    }
}
