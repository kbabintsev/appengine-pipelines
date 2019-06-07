package com.google.appengine.tools.pipeline.impl.util;

public final class ServiceUtils {
    private ServiceUtils() {
    }

    public static String getCurrentService() {
        final String service = System.getenv("GAE_SERVICE");
        return service == null ? System.getProperty("GAE_SERVICE") : service;
    }

    public static String getCurrentVersion() {
        final String version = System.getenv("GAE_VERSION");
        return version == null ? System.getProperty("GAE_VERSION") : version;
    }
}
