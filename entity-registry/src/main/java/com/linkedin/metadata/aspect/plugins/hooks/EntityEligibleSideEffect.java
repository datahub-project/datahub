package com.linkedin.metadata.aspect.plugins.hooks;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import javax.annotation.Nonnull;

/**
 * A side effect that acts only on the entity types it accepts.
 *
 * <p>An entity type the registry does not know is never eligible and never throws. Stored data can
 * hold such a type after a rollback to a version that lacks it, and a side effect must skip it
 * instead of failing the write.
 */
public interface EntityEligibleSideEffect {

  /** Whether the side effect acts on this entity type. */
  boolean isEntityEligible(@Nonnull EntitySpec entitySpec);

  /** Whether the side effect acts on this entity type, false when the registry does not know it. */
  default boolean isEntityTypeEligible(
      @Nonnull String entityType, @Nonnull EntityRegistry entityRegistry) {
    return entityRegistry.findEntitySpec(entityType).map(this::isEntityEligible).orElse(false);
  }
}
