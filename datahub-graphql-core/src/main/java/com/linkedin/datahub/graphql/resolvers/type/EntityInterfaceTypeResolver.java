/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.resolvers.type;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.TypeResolutionEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Entity} interface
 * type.
 */
public class EntityInterfaceTypeResolver implements TypeResolver {

  private final List<EntityType<?, ?>> _entities;

  public EntityInterfaceTypeResolver(final List<EntityType<?, ?>> entities) {
    _entities = entities;
  }

  @Override
  public GraphQLObjectType getType(TypeResolutionEnvironment env) {
    Object javaObject = env.getObject();
    final LoadableType<?, ?> filteredEntity =
        Iterables.getOnlyElement(
            _entities.stream()
                .filter(entity -> javaObject.getClass().isAssignableFrom(entity.objectClass()))
                .collect(Collectors.toList()));
    return env.getSchema().getObjectType(filteredEntity.objectClass().getSimpleName());
  }
}
