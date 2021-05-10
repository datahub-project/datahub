package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.EntitySpec;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Providers are responsible for resolving a set of Entity Specs whenever they are requested.
 */
public interface EntityRegistry {

    /**
     * Given an entity name, returns an instance of {@link EntitySpec}
     * @param entityName the name of the entity to be retrieve
     * @return an {@link EntitySpec} corresponding to the entity name provided, null if none exists.
     */
    EntitySpec getEntitySpec(@Nonnull final String entityName);


    /**
     * Returns all {@link EntitySpec}s that the register is aware of.
     * @return a list of {@link EntitySpec}s, empty list if none exists.
     */
    List<EntitySpec> getEntitySpecs();
}
