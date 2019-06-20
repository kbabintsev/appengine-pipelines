package com.google.appengine.tools.pipeline.impl.util;

import java.util.UUID;

public final class ValueStoragePath {
    private final UUID pipelineKey;
    private final String ownerType;
    private final UUID ownerKey;

    public ValueStoragePath(final UUID pipelineKey, final String ownerType, final UUID ownerKey) {
        this.pipelineKey = pipelineKey;
        this.ownerType = ownerType;
        this.ownerKey = ownerKey;
    }

    public String getPath() {
        return pipelineKey + "/" + ownerType + "-" + ownerKey + ".dat";
    }

    @Override
    public String toString() {
        return getPath();
    }
}
