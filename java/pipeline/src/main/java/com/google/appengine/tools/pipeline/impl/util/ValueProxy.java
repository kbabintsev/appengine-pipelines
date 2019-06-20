package com.google.appengine.tools.pipeline.impl.util;

import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.cloud.ByteArray;
import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public final class ValueProxy {
    private static final int DATABASE_VALUE_LIMIT = 5000000;
    private final PipelineManager pipelineManager;
    private final ValueStoragePath valueStoragePath;
    private ValueLocation valueLocation;
    private boolean valueLoaded;
    private Object value;

    public ValueProxy(
            final PipelineManager pipelineManager,
            final ValueLocation valueLocation,
            @Nullable final byte[] valueInDatabase,
            final boolean lazyLoading,
            final ValueStoragePath valueStoragePath
    ) {
        this.pipelineManager = pipelineManager;
        this.valueStoragePath = valueStoragePath;
        this.valueLocation = valueLocation;
        if (lazyLoading && valueLocation == ValueLocation.STORAGE) {
            value = null;
            valueLoaded = false;
        } else {
            try {
                if (valueLocation == ValueLocation.STORAGE) {
                    value = SerializationUtils.deserialize(pipelineManager.getBackEnd().retrieveBlob(valueStoragePath));
                } else if (valueLocation == ValueLocation.DATABASE) {
                    value = SerializationUtils.deserialize(valueInDatabase);
                } else {
                    throw new RuntimeException("Unknown ValueLocation: " + valueLocation);
                }
                valueLoaded = true;
            } catch (IOException e) {
                throw new RuntimeException("Can't deserialize value", e);
            }
        }
    }

    public ValueProxy(
            final PipelineManager pipelineManager,
            @Nullable final Object value,
            final ValueStoragePath valueStoragePath
    ) {
        this.pipelineManager = pipelineManager;
        this.valueStoragePath = valueStoragePath;
        this.value = value;
        final byte[] serialized = serializeValue(value);
        this.valueLocation = chooseLocation(serialized);
        this.valueLoaded = true;
    }

    private byte[] serializeValue(final Object valueIn) {
        final byte[] serialized;
        try {
            serialized = SerializationUtils.serialize(valueIn);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize value", e);
        }
        return serialized;
    }

    public void updateStorage(
            final Consumer<ValueLocation> locationConsumer,
            final Consumer<ByteArray> databaseBlobConsumer,
            final BiConsumer<ValueStoragePath, byte[]> storageBlobConsumer
    ) {
        if (value != null) {
            // if value is not null we need to make a choice where to store it
            final byte[] serialized = serializeValue(value);
            valueLocation = chooseLocation(serialized);
            locationConsumer.accept(valueLocation);
            if (valueLocation == ValueLocation.STORAGE) {
                if (valueLoaded) {
                    // we don't need to update storage if value wan't even loaded
                    storageBlobConsumer.accept(valueStoragePath, serialized);
                }
            } else {
                databaseBlobConsumer.accept(ByteArray.copyFrom(serialized));
            }
        } else {
            // nulls go into the database
            valueLocation = ValueLocation.DATABASE;
            locationConsumer.accept(valueLocation);
            databaseBlobConsumer.accept(null);
        }
    }

    private ValueLocation chooseLocation(final byte[] serialized) {
        return serialized.length < DATABASE_VALUE_LIMIT ? ValueLocation.DATABASE : ValueLocation.STORAGE;
    }

    public Object getValue() {
        if (!valueLoaded) {
            if (!ValueLocation.STORAGE.equals(valueLocation)) {
                throw new RuntimeException("Only " + ValueLocation.STORAGE + " supports lazy loading");
            }
            try {
                value = SerializationUtils.deserialize(pipelineManager.getBackEnd().retrieveBlob(valueStoragePath));
            } catch (IOException e) {
                throw new RuntimeException("Can't deserialize value", e);
            }
            valueLoaded = true;
        }
        return value;
    }

    public void setValue(final Object value) {
        this.value = value;
        this.valueLoaded = true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("valueStoragePath", valueStoragePath)
                .add("valueLocation", valueLocation)
                .add("value", value)
                .add("valueLoaded", valueLoaded)
                .toString();
    }
}
