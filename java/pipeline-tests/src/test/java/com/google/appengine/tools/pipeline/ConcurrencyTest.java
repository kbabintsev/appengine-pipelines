package com.google.appengine.tools.pipeline;

import java.util.UUID;

public class ConcurrencyTest extends PipelineTest {
    private static final int EXPECTED_RESULT = 123;

    public void testWaifForAllChildrenByDefault() throws Exception {
        UUID pipelineId = service.startNewPipeline(new GeneratorImmediateReturn());
        Integer result = waitForJobToComplete(pipelineId);
        assertEquals(EXPECTED_RESULT, result.intValue());
        assertEquals("TestSimpleCatchJob.run SimpleJob.run SimpleJob.run SimpleJob.run", trace());
        // Original framework would produce:
        // Actual   :TestSimpleCatchJob.run
        // After our fix it produces following, as it supposed to do:
        // Expected :TestSimpleCatchJob.run SimpleJob.run SimpleJob.run SimpleJob.run

    }

    public static class GeneratorImmediateReturn extends Job0<Integer> {

        private static final long serialVersionUID = 7608123241184342033L;
        private static final int RESULT = 123;

        @Override
        public Value<Integer> run() {
            trace("TestSimpleCatchJob.run");
            futureCall(new SimpleJob(), waitFor(newDelayedValue(1)));
            futureCall(new SimpleJob(), waitFor(newDelayedValue(2)));
            futureCall(new SimpleJob(), waitFor(newDelayedValue(3)));
            return immediate(RESULT);
        }
    }

    public static class SimpleJob extends Job0<Void> {

        private static final long serialVersionUID = 5692916638450145781L;

        @Override
        public Value<Void> run() {
            PipelineTest.trace("SimpleJob.run");
            return null;
        }
    }
}
