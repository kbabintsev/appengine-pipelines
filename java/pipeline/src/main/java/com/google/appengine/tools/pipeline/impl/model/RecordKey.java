package com.google.appengine.tools.pipeline.impl.model;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.UUID;

public final class RecordKey {
    private final UUID pipelineKey;
    private final UUID key;

    public RecordKey(final UUID pipelineKey, final UUID key) {
        this.pipelineKey = pipelineKey;
        this.key = key;
    }

    public RecordKey(final String serialized) {
        final String[] split = serialized.split(":");
        if (split.length != 2) {
            throw new RuntimeException("Can't deserialize RecordKey. Must be {UUID}:{UUID");
        }
        this.pipelineKey = UUID.fromString(split[0]);
        this.key = UUID.fromString(split[1]);
    }

    public UUID getPipelineKey() {
        return pipelineKey;
    }

    public UUID getKey() {
        return key;
    }

    public String sertialize() {
        return pipelineKey + ":" + key;
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
        return Objects.equal(pipelineKey, recordKey.pipelineKey)
                && Objects.equal(key, recordKey.key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(pipelineKey, key);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("pipelineKey", pipelineKey)
                .add("key", key)
                .toString();
    }
}
