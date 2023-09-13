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

package com.google.appengine.tools.pipeline.impl.servlets;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.appengine.tools.pipeline.impl.util.JsonUtils;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author rudominer@google.com (Mitch Rudominer)
 */
public final class JsonClassFilterHandler {

    static final String PATH_COMPONENT = "rpc/class_paths";
    private final PipelineManager pipelineManager;

    @Inject
    public JsonClassFilterHandler(final PipelineManager pipelineManager) {
        this.pipelineManager = pipelineManager;
    }

    public void doGet(@SuppressWarnings("unused") final HttpServletRequest req,
                      final HttpServletResponse resp) throws IOException {
        final Set<String> pipelines = pipelineManager.getRootPipelinesDisplayName();
        resp.getWriter().write(JsonUtils.mapToJson(Collections.singletonMap("classPaths", pipelines)));
    }
}
