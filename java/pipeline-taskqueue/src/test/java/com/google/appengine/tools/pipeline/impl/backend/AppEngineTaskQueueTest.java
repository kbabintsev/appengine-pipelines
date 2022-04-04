package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.api.taskqueue.TaskHandle;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.Route;
import com.google.appengine.tools.pipeline.impl.QueueSettings;
import com.google.appengine.tools.pipeline.impl.tasks.RunJobTask;
import com.google.appengine.tools.pipeline.impl.tasks.Task;
import com.google.appengine.tools.pipeline.impl.util.UuidGenerator;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * @author tkaitchuck
 */
public final class AppEngineTaskQueueTest extends TestCase {

    private LocalServiceTestHelper helper;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
        taskQueueConfig.setDisableAutoTaskExecution(true);
        taskQueueConfig.setShouldCopyApiProxyEnvironment(true);
        helper = new LocalServiceTestHelper(taskQueueConfig, new LocalModulesServiceTestConfig());
        helper.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        helper.tearDown();
        super.tearDown();
    }

    public void testEnqueueSingleTask() {
        final AppEngineTaskQueue queue = new AppEngineTaskQueue();
        final Task task = createTask();
        List<TaskHandle> handles = queue.addToQueue(Collections.singletonList(task));

        assertEquals(1, handles.size());
        assertEquals(task.getName(), handles.get(0).getName());

        handles = queue.addToQueue(Collections.singletonList(task));
        assertEquals(0, handles.size());
    }

    public void testEnqueueBatchTasks() {
        final AppEngineTaskQueue queue = new AppEngineTaskQueue();
        final List<Task> tasks = new ArrayList<>(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE);
        for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
            final Task task = createTask();
            tasks.add(task);
        }
        List<TaskHandle> handles = queue.addToQueue(tasks);
        assertEquals(AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE, handles.size());
        for (int i = 0; i < AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE; i++) {
            assertEquals(tasks.get(i).getName(), handles.get(i).getName());
        }

        handles = queue.addToQueue(tasks);
        assertEquals(0, handles.size());
    }

    public void testEnqueueLargeBatchTasks() {
        final AppEngineTaskQueue queue = new AppEngineTaskQueue();
        final int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2 + 10;
        final List<Task> tasks = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            final Task task = createTask();
            tasks.add(task);
        }
        List<TaskHandle> handles = queue.addToQueue(tasks);
        assertEquals(tasks.size(), handles.size());
        for (int i = 0; i < tasks.size(); i++) {
            assertEquals(tasks.get(i).getName(), handles.get(i).getName());
        }

        handles = queue.addToQueue(tasks);
        assertEquals(0, handles.size());
    }

    public void testEnqueueBatchTwoStages() {
        final AppEngineTaskQueue queue = new AppEngineTaskQueue();
        final int batchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE * 2;
        final List<Task> tasks = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            final Task task = createTask();
            tasks.add(task);
        }

        final int firstBatchSize = AppEngineTaskQueue.MAX_TASKS_PER_ENQUEUE;
        List<TaskHandle> handles = queue.addToQueue(tasks.subList(0, firstBatchSize));

        assertEquals(firstBatchSize, handles.size());
        for (int i = 0; i < firstBatchSize; i++) {
            assertEquals(tasks.get(i).getName(), handles.get(i).getName());
        }

        handles = queue.addToQueue(tasks);

        // Duplicate is rejected (not counted) per batch.
        final int expected = tasks.size() - firstBatchSize;
        assertEquals(expected, handles.size());
        for (int i = 0; i < expected; i++) {
            assertEquals(tasks.get(firstBatchSize + i).getName(), handles.get(i).getName());
        }
    }

    private Task createTask() {
        final UUID key = UuidGenerator.nextUuid();
        final Task task = new RunJobTask(key, key, new QueueSettings().setRoute(new Route().setVersion("m1")));
        return task;
    }
}
