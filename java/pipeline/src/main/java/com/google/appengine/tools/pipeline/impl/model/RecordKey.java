package com.google.appengine.tools.pipeline.impl.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.UUID;

public final class RecordKey {
    private final UUID rootJobKey;
    private final UUID key;

    public RecordKey(final UUID rootJobKey, final UUID key) {
        this.rootJobKey = rootJobKey;
        this.key = key;
    }

    public RecordKey(final String serialized) {
        final String[] split = serialized.split(":");
        if (split.length != 2) {
            throw new RuntimeException("Can't deserialize RecordKey. Must be {UUID}:{UUID");
        }
        this.rootJobKey = UUID.fromString(split[0]);
        this.key = UUID.fromString(split[1]);
    }

    public UUID getRootJobKey() {
        return rootJobKey;
    }

    public UUID getKey() {
        return key;
    }

    public String sertialize() {
        return rootJobKey + ":" + key;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordKey)) {
            return false;
        }
        final RecordKey recordKey = (RecordKey) o;
        return Objects.equal(rootJobKey, recordKey.rootJobKey)
                && Objects.equal(key, recordKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(rootJobKey, key);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("rootJobKey", rootJobKey)
                .add("key", key)
                .toString();
    }
}
