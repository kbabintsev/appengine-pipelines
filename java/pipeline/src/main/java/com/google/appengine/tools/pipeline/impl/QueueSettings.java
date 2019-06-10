package com.google.appengine.tools.pipeline.impl;

import com.google.appengine.tools.pipeline.Route;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

    private Route route;
    private String onQueue;
    private Long delay;

    /**
     * Merge will override any {@code null} setting with a matching setting from {@code other}.
     * Note, delay value is not being merged.
     */
    public QueueSettings merge(final QueueSettings other) {
        if (route == null) {
            route = other.getRoute();
        }
        if (onQueue == null) {
            onQueue = other.getOnQueue();
        }
        return this;
    }

    public Route getRoute() {
        return route;
    }

    public QueueSettings setRoute(final Route route) {
        this.route = route;
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
        return "QueueSettings[route=" + route + ", onQueue=" + onQueue + ", delayInSeconds=" + delay + "]";
    }
}
