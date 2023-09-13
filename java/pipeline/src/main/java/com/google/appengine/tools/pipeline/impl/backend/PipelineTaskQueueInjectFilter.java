package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.PipelineService;
import com.google.inject.Injector;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import java.io.IOException;

@Singleton
public class PipelineTaskQueueInjectFilter implements Filter {

    public static final String PIPELINE_SERVICE_ATTRIBUTE = PipelineTaskQueueInjectFilter.class.getName() + ".pipelineService";
    public static final String INJECTOR_ATTRIBUTE = PipelineTaskQueueInjectFilter.class.getName() + ".injector";

    private final PipelineService pipelineService;
    private final Injector injector;

    @Inject
    public PipelineTaskQueueInjectFilter(final PipelineService pipelineService, final Injector injector) {
        this.pipelineService = pipelineService;
        this.injector = injector;
    }

    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        request.setAttribute(PIPELINE_SERVICE_ATTRIBUTE, pipelineService);
        request.setAttribute(INJECTOR_ATTRIBUTE, injector);
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
