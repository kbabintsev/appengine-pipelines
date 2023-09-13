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

import com.google.appengine.tools.pipeline.impl.servlets.JsonListHandler;
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.when;

/**
 * Test for {@link JsonListHandlerTest}.
 */
public class JsonListHandlerTest extends PipelineTest {

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
        injector.getInstance(JsonListHandler.class).doGet(request, response);
        assertEquals("{\n  \"pipelines\" : [ ]\n}", output.toString());
    }

    @SuppressWarnings("unchecked")
    public void testHandlerWithResults() throws Exception {
        UUID pipelineId1 = service.startNewPipeline(new Main1Job());
        UUID pipelineId2 = service.startNewPipeline(new Main2Job(false));
        UUID pipelineId3 = service.startNewPipeline(new Main2Job(true),
                new JobSetting.BackoffSeconds(0), new JobSetting.MaxAttempts(2));
        String helloWorld = (String) waitForJobToComplete(pipelineId1);
        assertEquals("hello world", helloWorld);
        String hiThere = (String) waitForJobToComplete(pipelineId2);
        assertEquals("hi there", hiThere);
        String bla = (String) waitForJobToComplete(pipelineId3);
        assertEquals("bla", bla);
        injector.getInstance(JsonListHandler.class).doGet(request, response);
        Map<String, Object> results = (Map<String, Object>) JsonUtils.fromJson(output.toString());
        assertEquals(1, results.size());
        List<Map<String, Object>> pipelines = (List<Map<String, Object>>) results.get("pipelines");
        assertEquals(3, pipelines.size());
        Map<String, String> pipelineIdToClass = new HashMap<>();
        for (Map<String, Object> pipeline : pipelines) {
            pipelineIdToClass.put(
                    (String) pipeline.get("pipelineId"), (String) pipeline.get("classPath"));
        }
        assertEquals(Main1Job.class.getSimpleName(), pipelineIdToClass.get(pipelineId1.toString()));
        assertEquals(Main2Job.class.getSimpleName(), pipelineIdToClass.get(pipelineId2.toString()));
        assertEquals(Main2Job.class.getSimpleName(), pipelineIdToClass.get(pipelineId3.toString()));
    }

    @SuppressWarnings("serial")
    private static class Main1Job extends Job0<String> {

        @Override
        public Value<String> run() {
            FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hello"));
            FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" world"));
            return futureCall(new ConcatJob(), v1, v2);
        }
    }

    @SuppressWarnings("serial")
    private static class Main2Job extends Job0<String> {

        private final boolean shouldThrow;

        public Main2Job(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }

        @Override
        public Value<String> run() {
            if (shouldThrow) {
                throw new RuntimeException("bla");
            }
            FutureValue<String> v1 = futureCall(new StrJob<String>(), immediate("hi"));
            FutureValue<String> v2 = futureCall(new StrJob<String>(), immediate(" there"));
            return futureCall(new ConcatJob(), v1, v2);
        }

        @SuppressWarnings("unused")
        public Value<String> handleException(Throwable t) {
            return immediate(t.getMessage());
        }
    }

    @SuppressWarnings("serial")
    private static class ConcatJob extends Job2<String, String, String> {

        @Override
        public Value<String> run(String value1, String value2) {
            return immediate(value1 + value2);
        }
    }

    @SuppressWarnings("serial")
    private static class StrJob<T extends Serializable> extends Job1<String, T> {

        @Override
        public Value<String> run(T obj) {
            return immediate(obj == null ? "null" : obj.toString());
        }
    }
}
