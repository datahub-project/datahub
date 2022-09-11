package com.linkedin.metadata.entity;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.EventSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.template.AspectTemplateEngine;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class TestEntityRegistry implements EntityRegistry {

  private final Map<String, EntitySpec> entityNameToSpec;

  public TestEntityRegistry() {
    entityNameToSpec = new EntitySpecBuilder(EntitySpecBuilder.AnnotationExtractionMode.IGNORE_ASPECT_FIELDS)
        .buildEntitySpecs(new Snapshot().schema())
        .stream()
        .collect(Collectors.toMap(spec -> spec.getName().toLowerCase(), spec -> spec));
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

  @Nullable
  @Override
  public EventSpec getEventSpec(@Nonnull String eventName) {
    return null;
  }

  @Nonnull
  @Override
  public Map<String, EntitySpec> getEntitySpecs() {
    return entityNameToSpec;
  }

  @Nonnull
  @Override
  public Map<String, AspectSpec> getAspectSpecs() {
    return new HashMap<>();
  }

  @Nonnull
  @Override
  public Map<String, EventSpec> getEventSpecs() {
    return Collections.emptyMap();
  }

  @Nonnull
  @Override
  public AspectTemplateEngine getAspectTemplateEngine() {
    return new AspectTemplateEngine();
  }
}
