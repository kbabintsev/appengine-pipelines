package com.google.appengine.tools.pipeline.impl;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

    private String onBackend;
    private String onModule;
    private String moduleVersion;
    private String onQueue;
    private Long delay;

    /**
     * Merge will override any {@code null} setting with a matching setting from {@code other}.
     * Note, delay value is not being merged.
     */
    public QueueSettings merge(final QueueSettings other) {
        if (onBackend == null && onModule == null) {
            onBackend = other.getOnBackend();
        }
        if (onModule == null && onBackend == null) {
            onModule = other.getOnModule();
            moduleVersion = other.getModuleVersion();
        }
        if (onQueue == null) {
            onQueue = other.getOnQueue();
        }
        return this;
    }

    public String getOnBackend() {
        return onBackend;
    }

    public QueueSettings setOnBackend(final String onBackend) {
        if (onBackend != null && onModule != null) {
            throw new IllegalStateException("OnModule and OnBackend cannot be combined");
        }
        this.onBackend = onBackend;
        return this;
    }

    public String getOnModule() {
        return onModule;
    }

    public QueueSettings setOnModule(final String onModule) {
        if (onModule != null && onBackend != null) {
            throw new IllegalStateException("OnModule and OnBackend cannot be combined");
        }
        this.onModule = onModule;
        return this;
    }

    public String getModuleVersion() {
        return moduleVersion;
    }

    public QueueSettings setModuleVersion(final String moduleVersion) {
        this.moduleVersion = moduleVersion;
        return this;
    }

    public String getOnQueue() {
        return onQueue;
    }

    public QueueSettings setOnQueue(final String onQueue) {
        this.onQueue = onQueue;
        return this;
    }

    public Long getDelayInSeconds() {
        return delay;
    }

    public void setDelayInSeconds(final Long delayInSeconds) {
        this.delay = delayInSeconds;
    }

    @Override
    public QueueSettings clone() {
        try {
            return (QueueSettings) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Should never happen", e);
        }
    }

    @Override
    public String toString() {
        return "QueueSettings[onBackEnd=" + onBackend + ", onModule=" + onModule + ", moduleVersion="
                + moduleVersion + ", onQueue=" + onQueue + ", delayInSeconds=" + delay + "]";
    }
}
