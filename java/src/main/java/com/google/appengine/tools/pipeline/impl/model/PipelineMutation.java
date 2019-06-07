package com.google.appengine.tools.pipeline.impl.model;

import com.google.cloud.spanner.Mutation;

import java.util.UUID;

public final class PipelineMutation {
    private final Mutation.WriteBuilder databaseMutation;
    private BlobMutation blobMutation;

    public PipelineMutation(final Mutation.WriteBuilder databaseMutation) {
        this.databaseMutation = databaseMutation;
    }

    public Mutation.WriteBuilder getDatabaseMutation() {
        return databaseMutation;
    }

    public BlobMutation getBlobMutation() {
        return blobMutation;
    }

    public void setBlobMutation(final BlobMutation blobMutation) {
        if (this.blobMutation != null) {
            throw new RuntimeException("Somebody already set blobMutation");
        }
        this.blobMutation = blobMutation;
    }

    public static final class BlobMutation {
        private UUID rootJobKey;
        private String type;
        private UUID key;
        private byte[] value;

        public BlobMutation(final UUID rootJobKey, final String type, final UUID key, final byte[] value) {
            this.rootJobKey = rootJobKey;
            this.type = type;
            this.key = key;
            this.value = value;
        }

        public UUID getRootJobKey() {
            return rootJobKey;
        }

        public String getType() {
            return type;
        }

        public UUID getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }

}
