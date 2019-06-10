package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.PipelineService;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

@Singleton
public class PipelineTaskQueueInjectFilter implements Filter {

    public static final String PIPELINE_SERVICE_ATTRIBUTE = PipelineTaskQueueInjectFilter.class.getName() + ".pipelineService";

    private final PipelineService pipelineService;

    @Inject
    public PipelineTaskQueueInjectFilter(final PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        request.setAttribute(PIPELINE_SERVICE_ATTRIBUTE, pipelineService);
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
