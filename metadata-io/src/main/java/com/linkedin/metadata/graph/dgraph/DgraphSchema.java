package com.linkedin.metadata.graph.dgraph;

import io.dgraph.DgraphProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Provides a thread-safe Dgraph schema. Returned data structures are immutable.
 */
@Slf4j
public class DgraphSchema {
    private final @Nonnull Set<String> fields;
    private final @Nonnull Map<String, Set<String>> types;
    private final DgraphExecutor dgraph;

    public static DgraphSchema empty() {
        return new DgraphSchema(Collections.emptySet(), Collections.emptyMap(), null);
    }

    public DgraphSchema(@Nonnull Set<String> fields, @Nonnull Map<String, Set<String>> types) {
        this(fields, types, null);
    }

    public DgraphSchema(@Nonnull Set<String> fields, @Nonnull Map<String, Set<String>> types, DgraphExecutor dgraph) {
        this.fields = fields;
        this.types = types;
        this.dgraph = dgraph;
    }

    /**
     * Adds the given DgraphExecutor to this schema returning a new instance.
     * Be aware this and the new instance share the underlying fields and types datastructures.
     *
     * @param dgraph dgraph executor to add
     * @return new instance
     */
    public DgraphSchema withDgraph(DgraphExecutor dgraph) {
        return new DgraphSchema(this.fields, this.types, dgraph);
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

    synchronized public void ensureField(String typeName, String fieldName, String... existingFieldNames) {
        // quickly check if the field is known for this type
        if (hasField(typeName, fieldName)) {
            return;
        }

        // add type and field to schema
        StringJoiner schema = new StringJoiner("\n");

        if (!fields.contains(fieldName)) {
            schema.add(String.format("<%s>: [uid] @reverse .", fieldName));
        }

        // update the schema on the Dgraph cluster
        Set<String> allTypesFields = new HashSet<>(Arrays.asList(existingFieldNames));
        allTypesFields.addAll(types.getOrDefault(typeName, Collections.emptySet()));
        allTypesFields.add(fieldName);

        if (dgraph != null) {
            log.info("Adding predicate {} for type {} to schema", fieldName, typeName);

            StringJoiner type = new StringJoiner("\n  ");
            allTypesFields.stream().map(t -> "<" + t + ">").forEach(type::add);
            schema.add(String.format("type <%s> {\n  %s\n}", typeName, type));
            log.debug("Adding to schema: " + schema);
            DgraphProto.Operation setSchema = DgraphProto.Operation.newBuilder().setSchema(schema.toString()).setRunInBackground(true).build();
            dgraph.executeConsumer(dgraphClient -> dgraphClient.alter(setSchema));
        }

        // now that the schema has been updated on dgraph we can cache this new type / field
        // ensure type and fields of type exist
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
