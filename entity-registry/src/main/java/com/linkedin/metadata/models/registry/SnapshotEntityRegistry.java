package com.linkedin.metadata.models.registry;

import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Implementation of {@link EntityRegistry} that builds {@link DefaultEntitySpec} objects
 * from the a {@link Snapshot} Record Template present on the classpath
 */
public class SnapshotEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;
  private final List<EntitySpec> entitySpecs;

  private static final SnapshotEntityRegistry INSTANCE = new SnapshotEntityRegistry();

  public SnapshotEntityRegistry() {
    entityNameToSpec = new EntitySpecBuilder().buildEntitySpecs(new Snapshot().schema())
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
  }

  public SnapshotEntityRegistry(UnionTemplate snapshot) {
    entityNameToSpec = new EntitySpecBuilder().buildEntitySpecs(snapshot.schema())
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
    entitySpecs = new ArrayList<>(entityNameToSpec.values());
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull final String entityName) {
    String lowercaseEntityName = entityName.toLowerCase();
    if (!entityNameToSpec.containsKey(lowercaseEntityName)) {
      throw new IllegalArgumentException(
          String.format("Failed to find entity with name %s in EntityRegistry", entityName));
    }
    return entityNameToSpec.get(lowercaseEntityName);
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return new HashMap<>();
  }

  @Override
  public EventSpec getEventSpec(final String ignored) {
    return null;
  }

  public static SnapshotEntityRegistry getInstance() {
    return INSTANCE;
  }
}
