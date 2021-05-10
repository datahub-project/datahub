package com.linkedin.metadata.models.registry;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.snapshot.Snapshot;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link EntityRegistry} that parses {@link EntitySpec} objects
 * from the a {@link Snapshot} Record Template present on the classpath
 */
public class SnapshotEntityRegistry implements EntityRegistry {

    private final Map<String, EntitySpec> entityNameToSpec;

    public SnapshotEntityRegistry() {
        entityNameToSpec = EntitySpecBuilder.buildEntitySpecs(new Snapshot().schema())
                .stream()
                .collect(Collectors.toMap(EntitySpec::getName, spec -> spec));
    }

    @Override
    public EntitySpec getEntitySpec(@Nonnull final String entityName) {
        if (!entityNameToSpec.containsKey(entityName)) {
            throw new IllegalArgumentException(String.format("Failed to find entity with name %s in EntityRegistry", entityName));
        }
        return entityNameToSpec.get(entityName);
    }

    @Override
    public List<EntitySpec> getEntitySpecs() {
        return new ArrayList<>(entityNameToSpec.values());
    }

    public EntitySpec getEntitySpec(@Nonnull final RecordDataSchema entitySnapshotSchema) {
        final String entityName = getEntityNameFromSnapshot(entitySnapshotSchema);
        return getEntitySpec(entityName);
    }

    private String getEntityNameFromSnapshot(final RecordDataSchema entitySnapshotSchema) {
        final Object entityAnnotationObj = entitySnapshotSchema.getProperties().get("Entity");
        if (entityAnnotationObj != null) {
            return EntityAnnotation.fromSchemaProperty(entityAnnotationObj).getName();
        }
        throw new IllegalArgumentException(String.format("Failed to extract entity name from provided schema %s",
                entitySnapshotSchema.getName()));
    }
}
