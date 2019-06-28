package com.google.appengine.tools.pipeline;

public final class Consts {
    public static final String BLOB_BUCKET_NAME;
    public static final String SPANNER_PROJECT;
    public static final String SPANNER_INSTANCE;
    public static final String SPANNER_DATABASE;

    static {
        BLOB_BUCKET_NAME = System.getProperty("com.google.appengine.tools.pipeline.BLOB_BUCKET_NAME");
        SPANNER_PROJECT = System.getProperty("com.google.appengine.tools.pipeline.SPANNER_PROJECT");
        SPANNER_INSTANCE = System.getProperty("com.google.appengine.tools.pipeline.SPANNER_INSTANCE");
        SPANNER_DATABASE = System.getProperty("com.google.appengine.tools.pipeline.SPANNER_DATABASE");
    }

    private Consts() {
    }
}
