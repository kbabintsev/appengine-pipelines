package com.google.appengine.tools.pipeline.impl.util;

import java.util.UUID;

public final class ValueStoragePath {
    private final UUID rootJobKey;
    private final String ownerType;
    private final UUID ownerKey;

    public ValueStoragePath(final UUID rootJobKey, final String ownerType, final UUID ownerKey) {
        this.rootJobKey = rootJobKey;
        this.ownerType = ownerType;
        this.ownerKey = ownerKey;
    }

    public String getPath() {
        return rootJobKey + "/" + ownerType + "-" + ownerKey + ".dat";
    }

    @Override
    public String toString() {
        return getPath();
    }
}
