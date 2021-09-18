package com.linkedin.metadata.graph;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides a thread-safe Dgraph schema. Returned data structures are immutable.
 */
public class DgraphSchema {
    private final @Nonnull Set<String> fields;
    private final @Nonnull Map<String, Set<String>> types;

    public DgraphSchema(@Nonnull Set<String> fields, @Nonnull Map<String, Set<String>> types) {
        this.fields = fields;
        this.types = types;
    }

    synchronized public boolean isEmpty() {
        return fields.isEmpty();
    }

    synchronized public Set<String> getFields() {
        // Provide an unmodifiable copy
        return Collections.unmodifiableSet(new HashSet<>(fields));
    }

    synchronized public Set<String> getFields(String typeName) {
        // Provide an unmodifiable copy
        return Collections.unmodifiableSet(new HashSet<>(types.getOrDefault(typeName, Collections.emptySet())));
    }

    synchronized public Map<String, Set<String>> getTypes() {
        // Provide an unmodifiable copy of the map and contained sets
        return Collections.unmodifiableMap(
                new HashSet<>(types.entrySet()).stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> Collections.unmodifiableSet(new HashSet<>(e.getValue()))
                        ))
        );
    }

    synchronized public boolean hasType(String typeName) {
        return types.containsKey(typeName);
    }

    synchronized public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    synchronized public boolean hasField(String typeName, String fieldName) {
        return types.getOrDefault(typeName, Collections.emptySet()).contains(fieldName);
    }

    synchronized public void addField(String typeName, String fieldName) {
        if (!types.containsKey(typeName)) {
            types.put(typeName, new HashSet<>());
        }
        types.get(typeName).add(fieldName);
        fields.add(fieldName);
    }

    synchronized public void clear() {
        types.clear();
        fields.clear();
    }
}
