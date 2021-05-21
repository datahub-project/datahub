package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.EntitySpec;
import java.util.List;
import javax.annotation.Nonnull;


/**
 * The Entity Registry provides a mechanism to retrieve metadata about entities modeled in GMA.
 * Metadata includes the entity's common name, the aspects that comprise it, and search index +
 * relationship index information about the entity.
 */
public interface EntityRegistry {

  /**
   * Given an entity name, returns an instance of {@link EntitySpec}
   * @param entityName the name of the entity to be retrieved
   * @return an {@link EntitySpec} corresponding to the entity name provided, null if none exists.
   */
  @Nonnull
  EntitySpec getEntitySpec(@Nonnull final String entityName);

  /**
   * Returns all {@link EntitySpec}s that the register is aware of.
   * @return a list of {@link EntitySpec}s, empty list if none exists.
   */
  @Nonnull
  List<EntitySpec> getEntitySpecs();
}
