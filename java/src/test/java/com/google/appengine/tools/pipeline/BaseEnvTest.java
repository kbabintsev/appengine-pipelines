package com.google.appengine.tools.pipeline;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.RuntimeConfigBuilder;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.config.io.ProcessOutput;
import de.flapdoodle.embed.process.io.Processors;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.appengine.tools.pipeline.impl.util.GUIDGenerator.USE_SIMPLE_GUIDS_FOR_DEBUGGING;

public class BaseEnvTest {
  private static final int DEFAULT_MONGO_TEST_PORT = 12345;
  private static LocalDatastoreHelper localDatastoreHelper = LocalDatastoreHelper.create();
  private static MongodExecutable mongodExecutable;
  protected LocalServiceTestHelper helper;
  protected LocalTaskQueue taskQueue;


  public BaseEnvTest() {
    LocalTaskQueueTestConfig taskQueueConfig = new LocalTaskQueueTestConfig();
    taskQueueConfig.setCallbackClass(TestingTaskQueueCallback.class);
    taskQueueConfig.setDisableAutoTaskExecution(false);

    helper = new LocalServiceTestHelper(taskQueueConfig, new LocalModulesServiceTestConfig());
  }

  @BeforeClass
  public static void setUpClass() {
    try {
      localDatastoreHelper.start();
      System.setProperty(com.google.datastore.v1.client.DatastoreHelper.LOCAL_HOST_ENV_VAR, localDatastoreHelper.getOptions().getHost());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    try {
      Logger logger = Logger.getLogger(BaseEnvTest.class.getName());
      ProcessOutput processOutput = new ProcessOutput(Processors.logTo(logger, Level.INFO), Processors.logTo(logger,
          Level.SEVERE), Processors.named("[console>]", Processors.logTo(logger, Level.FINE)));
      IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
          .defaultsWithLogger(Command.MongoD, logger)
          .processOutput(processOutput)
          .build();
      MongodStarter mongodStarter = MongodStarter.getInstance(runtimeConfig);
      mongodExecutable = null;
      final IMongodConfig mongodConfig = new MongodConfigBuilder()
          .version(Version.Main.PRODUCTION)
          .net(new Net(DEFAULT_MONGO_TEST_PORT, Network.localhostIsIPv6()))
          .build();
      mongodExecutable = mongodStarter.prepare(mongodConfig);
      mongodExecutable.start();
      System.setProperty(PipelineManager.MONGODB_URI, "mongodb://localhost:" + DEFAULT_MONGO_TEST_PORT);
      System.setProperty(PipelineManager.MONGODB_DEFAULT_PROJECT_ID, "test-project");

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @AfterClass
  public static void tearDownClass() {
    try {
      localDatastoreHelper.stop(Duration.ofSeconds(2));
      mongodExecutable.stop();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }
  }

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    localDatastoreHelper.reset();
    System.setProperty(USE_SIMPLE_GUIDS_FOR_DEBUGGING, "true");
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }
}
