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

import com.google.appengine.tools.pipeline.impl.servlets.JsonClassFilterHandler;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * Test for {@link JsonClassFilterHandler}.
 */
public final class JsonClassFilterHandlerTest extends PipelineTest {

    private final StringWriter output = new StringWriter();
    @Mock
    private HttpServletRequest request;
    @Mock
    private HttpServletResponse response;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        when(response.getWriter()).thenReturn(new PrintWriter(output));
    }

    public void testHandlerNoResults() throws Exception {
        injector.getInstance(JsonClassFilterHandler.class).doGet(request, response);
        assertEquals("{" + System.lineSeparator() + "  \"classPaths\" : [ ]" + System.lineSeparator() + "}", output.toString());
    }

    public void testHandlerWithResults() throws Exception {
        final UUID pipelineId1 = service.startNewPipeline(new Main1Job());
        final UUID pipelineId2 = service.startNewPipeline(new Main2Job(false));
        final UUID pipelineId3 = service.startNewPipeline(new Main2Job(true),
                new JobSetting.BackoffSeconds(0), new JobSetting.MaxAttempts(2));
        final String helloWorld = (String) waitForJobToComplete(pipelineId1);
        assertEquals("hello world", helloWorld);
        final String hiThere = (String) waitForJobToComplete(pipelineId2);
        assertEquals("hi there", hiThere);
        final String bla = (String) waitForJobToComplete(pipelineId3);
        assertEquals("bla", bla);
        injector.getInstance(JsonClassFilterHandler.class).doGet(request, response);
        System.out.println(output.toString());
        final String expected = "{" + System.lineSeparator()
                + "  \"classPaths\" : ["
                + " \"" + Main1Job.class.getSimpleName() + "\", "
                + "\"" + Main2Job.class.getSimpleName() + "\" "
                + "]" + System.lineSeparator() + "}";
        assertEquals(expected, output.toString());
    }

    @SuppressWarnings("serial")
    private static class Main1Job extends Job0<String> {

        @Override
        public Value<String> run() {
            final FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hello"));
            final FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" world"));
            return futureCall(new ConcatJob(), v1, v2);
        }
    }

    @SuppressWarnings("serial")
    private static class Main2Job extends Job0<String> {

        private final boolean shouldThrow;

        Main2Job(final boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }

        @Override
        public Value<String> run() {
            if (shouldThrow) {
                throw new RuntimeException("bla");
            }
            final FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hi"));
            final FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" there"));
            return futureCall(new ConcatJob(), v1, v2);
        }

        @SuppressWarnings("unused")
        public Value<String> handleException(final Throwable t) {
            return immediate(t.getMessage());
        }
    }

    @SuppressWarnings("serial")
    private static class ConcatJob extends Job2<String, String, String> {

        @Override
        public Value<String> run(final String value1, final String value2) {
            return immediate(value1 + value2);
        }
    }

    @SuppressWarnings("serial")
    private static class StrJob<T extends Serializable> extends Job1<String, T> {

        @Override
        public Value<String> run(final T obj) {
            return immediate(obj == null ? "null" : obj.toString());
        }
    }
}
