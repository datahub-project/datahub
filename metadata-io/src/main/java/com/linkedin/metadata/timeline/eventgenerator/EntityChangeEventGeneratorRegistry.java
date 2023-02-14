package com.linkedin.metadata.timeline.eventgenerator;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * A registry that maps an aspect name to one or more {@link EntityChangeEventGenerator}s.
 */
public class EntityChangeEventGeneratorRegistry {

  private final Map<String, Set<EntityChangeEventGenerator<?>>> entityChangeEventGenerators = new HashMap<>();

  /**
   * Registers a new EntityChangeEventGenerator for the given aspect.
   */
  public void register(@Nonnull final String aspectName,
      @Nonnull final EntityChangeEventGenerator<?> entityChangeEventGenerator) {
    Objects.requireNonNull(aspectName);
    Objects.requireNonNull(entityChangeEventGenerator);
    entityChangeEventGenerators.putIfAbsent(aspectName, new HashSet<>());
    entityChangeEventGenerators.get(aspectName).add(entityChangeEventGenerator);
  }

  /**
   * Registers a new Enity Change Generator, or null if one does not exist.
   */
  public Set<EntityChangeEventGenerator<?>> getEntityChangeEventGenerators(@Nonnull final String aspectName) {
    final String key = Objects.requireNonNull(aspectName);
    return this.entityChangeEventGenerators.getOrDefault(key, new HashSet<>());
  }
}
