package com.linkedin.datahub.graphql.types;

import com.linkedin.datahub.graphql.generated.Entity;
import java.util.function.Function;

/**
 * GQL graph type representing a top-level GMS entity (eg. Dataset, User, DataPlatform, Chart,
 * etc.).
 *
 * @param <T>: The GraphQL object type corresponding to the entity, must be of type {@link Entity}
 * @param <K> the key type for the DataLoader
 */
public interface EntityType<T extends Entity, K> extends LoadableType<T, K> {

  /**
   * Retrieves the {@link com.linkedin.datahub.graphql.generated.EntityType} associated with the
   * Graph type, eg. 'DATASET'
   */
  com.linkedin.datahub.graphql.generated.EntityType type();

  Function<Entity, K> getKeyProvider();
}
