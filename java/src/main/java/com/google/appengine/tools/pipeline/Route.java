package com.google.appengine.tools.pipeline;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Map;

public final class Route implements Serializable {

    private static final long serialVersionUID = -8983675245350589029L;

    private String service;
    private String version;
    private String instance;
    private Map<String, String> headers;

    public String getService() {
        return service;
    }

    public Route setService(final String service) {
        this.service = service;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Route setVersion(final String version) {
        this.version = version;
        return this;
    }

    public String getInstance() {
        return instance;
    }

    public Route setInstance(final String instance) {
        this.instance = instance;
        return this;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public Route setHeaders(final Map<String, String> headers) {
        this.headers = headers;
        return this;
    }

    public Route addHeader(final String name, final String value) {
        if (headers == null) {
            headers = Maps.newLinkedHashMap();
        }
        headers.put(name, value);
        return this;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("service", service)
                .add("version", version)
                .add("instance", instance)
                .add("headers", headers)
                .toString();
    }
}
