package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Combines results from two entity registries, where the second takes precedence
 */
public class MergedEntityRegistry implements EntityRegistry {
  private final Map<String, EntitySpec> entityNameToSpec;

  public MergedEntityRegistry(EntityRegistry e1, EntityRegistry e2) {
    entityNameToSpec = new HashMap<>(e1.getEntitySpecs());
    for (Map.Entry<String, EntitySpec> e2Entry : e2.getEntitySpecs().entrySet()) {
      if (entityNameToSpec.containsKey(e2Entry.getKey())) {
        EntitySpec mergeEntitySpec = mergeEntitySpecs(entityNameToSpec.get(e2Entry.getKey()), e2Entry.getValue());
        entityNameToSpec.put(e2Entry.getKey(), mergeEntitySpec);
      } else {
        entityNameToSpec.put(e2Entry.getKey(), e2Entry.getValue());
      }
    }
    //TODO: Validate that the entity registries don't have conflicts among each other
  }

  private EntitySpec mergeEntitySpecs(EntitySpec entitySpec1, EntitySpec entitySpec2) {
    Map<String, AspectSpec> aspectSpecMap = new HashMap<>(entitySpec1.getAspectSpecMap());
    aspectSpecMap.putAll(entitySpec2.getAspectSpecMap());
    return new DefaultEntitySpec(aspectSpecMap.values(), entitySpec1.getEntityAnnotation(),
        entitySpec1.getSnapshotSchema(), entitySpec1.getAspectTyperefSchema());
  }

  @Nonnull
  @Override
  public EntitySpec getEntitySpec(@Nonnull String entityName) {
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
}
