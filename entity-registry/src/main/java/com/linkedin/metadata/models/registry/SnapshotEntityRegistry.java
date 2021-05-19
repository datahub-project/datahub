package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Implementation of {@link EntityRegistry} that parses {@link EntitySpec} objects
 * from the a {@link Snapshot} Record Template present on the classpath
 */
public class SnapshotEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;

  private static final SnapshotEntityRegistry INSTANCE = new SnapshotEntityRegistry();

  private SnapshotEntityRegistry() {
    entityNameToSpec = EntitySpecBuilder.buildEntitySpecs(new Snapshot().schema(), EntitySpecBuilder.ValidationMode.WARN)
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
  }

  @Override
  public EntitySpec getEntitySpec(@Nonnull final String entityName) {
    String lowercaseEntityName = entityName.toLowerCase();
    if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find entity with name %s in EntityRegistry", entityName));
    }
    return entityNameToSpec.get(lowercaseEntityName);
  }

  @Override
  public List<EntitySpec> getEntitySpecs() {
    return new ArrayList<>(entityNameToSpec.values());
  }

  public static SnapshotEntityRegistry getInstance() {
    return INSTANCE;
  }
}
