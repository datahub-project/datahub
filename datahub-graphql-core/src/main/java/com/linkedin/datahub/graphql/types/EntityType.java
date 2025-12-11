/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
