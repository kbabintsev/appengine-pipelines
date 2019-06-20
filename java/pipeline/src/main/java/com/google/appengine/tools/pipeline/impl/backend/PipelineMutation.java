package com.google.appengine.tools.pipeline.impl.backend;

import com.google.appengine.tools.pipeline.impl.util.ValueStoragePath;
import com.google.cloud.spanner.Mutation;

public final class PipelineMutation {
    private final Mutation.WriteBuilder databaseMutation;
    private ValueMutation valueMutation;

    public PipelineMutation(final Mutation.WriteBuilder databaseMutation) {
        this.databaseMutation = databaseMutation;
    }

    public Mutation.WriteBuilder getDatabaseMutation() {
        return databaseMutation;
    }

    public ValueMutation getValueMutation() {
        return valueMutation;
    }

    public void setValueMutation(final ValueMutation valueMutation) {
        if (this.valueMutation != null) {
            throw new RuntimeException("Somebody already set valueMutation");
        }
        this.valueMutation = valueMutation;
    }

    public static final class ValueMutation {
        private final ValueStoragePath location;
        private final byte[] value;

        public ValueMutation(final ValueStoragePath location, final byte[] value) {
            this.location = location;
            this.value = value;
        }

        public ValueStoragePath getPath() {
            return location;
        }

        public byte[] getValue() {
            return value;
        }
    }

}
