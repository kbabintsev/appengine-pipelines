package com.google.appengine.tools.pipeline.impl;

import com.google.common.base.MoreObjects;

/**
 * Queue settings implementation.
 *
 * @author ozarov@google.com (Arie Ozarov)
 */
public final class QueueSettings implements Cloneable {

  private String onBackend;
  private String onQueue;
  private String onService;
  private String onVersion;
  private Long delay;

  /**
   * Merge will override any {@code null} setting with a matching setting from {@code other}.
   * Note, delay value is not being merged. moduleVersion is only copied if onModule is copied.
   */
  public QueueSettings merge(QueueSettings other) {
    if (onBackend == null) {
      onBackend = other.getOnBackend();
    }
    if (onService == null) {
      onService = other.getOnService();
    }
    if (onVersion == null) {
      onVersion = other.getOnVersion();
    }
    if (onQueue == null) {
      onQueue = other.getOnQueue();
    }
    return this;
  }

  public QueueSettings setOnBackend(String onBackend) {
    this.onBackend = onBackend;
    return this;
  }

  public String getOnBackend() {
    return onBackend;
  }

  public String getOnService() {
    return onService;
  }

  public QueueSettings setOnService(final String onService) {
    this.onService = onService;
    return this;
  }

  public String getOnVersion() {
    return onVersion;
  }

  public QueueSettings setOnVersion(final String onVersion) {
    this.onVersion = onVersion;
    return this;
  }

  public QueueSettings setOnQueue(String onQueue) {
    this.onQueue = onQueue;
    return this;
  }


  public String getOnQueue() {
    return onQueue;
  }

  public void setDelayInSeconds(Long delay) {
    this.delay = delay;
  }

  public Long getDelayInSeconds() {
    return delay;
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
    return MoreObjects.toStringHelper(this)
            .add("onBackend", onBackend)
            .add("onService", onService)
            .add("onVersion", onVersion)
            .add("onQueue", onQueue)
            .toString();
  }
}
