package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.DefaultEntitySpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * The Entity Registry provides a mechanism to retrieve metadata about entities modeled in GMA.
 * Metadata includes the entity's common name, the aspects that comprise it, and search index +
 * relationship index information about the entity.
 */
public interface EntityRegistry {

  default String getIdentifier() {
    return "Unknown";
  }

  /**
   * Given an entity name, returns an instance of {@link DefaultEntitySpec}
   * @param entityName the name of the entity to be retrieved
   * @return an {@link DefaultEntitySpec} corresponding to the entity name provided, null if none exists.
   */
  @Nonnull
  EntitySpec getEntitySpec(@Nonnull final String entityName);

  /**
   * Given an event name, returns an instance of {@link DefaultEventSpec}.
   *
   * @param eventName the name of the event to be retrieved
   * @return an {@link DefaultEventSpec} corresponding to the entity name provided, null if none exists.
   */
  @Nullable
  EventSpec getEventSpec(@Nonnull final String eventName);

  /**
   * Returns all {@link DefaultEntitySpec}s that the register is aware of.
   * @return a list of {@link DefaultEntitySpec}s, empty list if none exists.
   */
  @Nonnull
  Map<String, EntitySpec> getEntitySpecs();

  /**
   * Returns all {@link EventSpec}s that the registry is aware of.
   */
  @Nonnull
  Map<String, EventSpec> getEventSpecs();
}
