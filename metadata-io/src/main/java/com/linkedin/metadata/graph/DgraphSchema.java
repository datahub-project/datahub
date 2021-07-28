package com.linkedin.metadata.graph;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.stream.Collectors;

public class DgraphSchema {
    private final @Nonnull Set<String> fields;
    private final @Nonnull Map<String, Set<String>> types;

    public DgraphSchema(@Nonnull Set<String> fields, @Nonnull Map<String, Set<String>> types) {
        this.fields = fields;
        this.types = types;
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public Set<String> getFields() {
        // Make Set unmodifiable
        return Collections.unmodifiableSet(fields);
    }

    public Set<String> getFields(String typeName) {
        // Make Set unmodifiable
        return Collections.unmodifiableSet(types.getOrDefault(typeName, Collections.emptySet()));
    }

    public Map<String, Set<String>> getTypes() {
        // make Map and contained sets unmodifiable
        return Collections.unmodifiableMap(
                types.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> Collections.unmodifiableSet(e.getValue())
                        ))
        );
    }

    public boolean hasType(String typeName) {
        return types.containsKey(typeName);
    }

    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    public boolean hasField(String typeName, String fieldName) {
        return types.getOrDefault(typeName, Collections.emptySet()).contains(fieldName);
    }

    public void addField(String typeName, String fieldName) {
        if (!types.containsKey(typeName))
            types.put(typeName, new HashSet<>());
        types.get(typeName).add(fieldName);
        fields.add(fieldName);
    }
}
