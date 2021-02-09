package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.generated.Entity;

/**
 * GQL graph type representing a top-level GMS entity (eg. Dataset, User, DataPlatform, Chart, etc.).
 *
 * @param <T>: The GraphQL object type corresponding to the entity, must be of type {@link Entity}
 */
public interface EntityType<T extends Entity> extends LoadableType<T> {

    /**
     * Retrieves the {@link com.linkedin.datahub.graphql.generated.EntityType} associated with the Graph type, eg. 'DATASET'
     */
    com.linkedin.datahub.graphql.generated.EntityType type();

}
