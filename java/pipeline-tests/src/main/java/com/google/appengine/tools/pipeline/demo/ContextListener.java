package com.google.appengine.tools.pipeline.demo;

import com.google.appengine.tools.pipeline.PipelineModule;
//import com.google.appengine.tools.pipeline.impl.backend.AppEngineTaskQueueModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;

public final class ContextListener extends com.google.inject.servlet.GuiceServletContextListener {

    @Override
    protected Injector getInjector() {
        return Guice.createInjector(
                // new AppEngineTaskQueueModule(),
                new PipelineModule()
        );
    }

    @Override
    public void contextInitialized(final ServletContextEvent servletContextEvent) {
        // No call to super as it also calls getInjector()
        final ServletContext sc = servletContextEvent.getServletContext();
        sc.setAttribute(Injector.class.getName(), getInjector());
    }

    @Override
    public void contextDestroyed(final ServletContextEvent servletContextEvent) {
        final ServletContext sc = servletContextEvent.getServletContext();
        sc.removeAttribute(Injector.class.getName());
        super.contextDestroyed(servletContextEvent);
    }
}
