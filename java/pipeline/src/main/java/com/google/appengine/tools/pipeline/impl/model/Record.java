package com.google.appengine.tools.pipeline.impl.model;

import com.google.appengine.tools.pipeline.impl.backend.PipelineMutation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public interface Record {
    static List<String> propertiesForSelect(@Nonnull final String tableName, @Nonnull final List<String> properties, @Nullable final String prefix) {
        if (prefix == null) {
            return properties;
        }
        return properties.stream().map(p -> tableName + "." + p + " as " + prefix + p).collect(Collectors.toList());
    }

    static String property(@Nullable final String prefix, @Nonnull final String property) {
        return prefix == null ? property : (prefix + property);
    }

    String getDatastoreKind();

    PipelineMutation toEntity();

}
