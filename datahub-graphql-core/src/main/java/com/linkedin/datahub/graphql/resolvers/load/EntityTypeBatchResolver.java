package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.resolvers.BatchLoadUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * GraphQL resolver responsible for
 *
 * <p>1. Retrieving a single input urn. 2. Resolving a single Entity
 */
public class EntityTypeBatchResolver implements DataFetcher<CompletableFuture<List<Entity>>> {

  private final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> _entityTypes;
  private final Function<DataFetchingEnvironment, List<Entity>> _entitiesProvider;

  public EntityTypeBatchResolver(
      final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
      final Function<DataFetchingEnvironment, List<Entity>> entitiesProvider) {
    _entityTypes = entityTypes;
    _entitiesProvider = entitiesProvider;
  }

  @Override
  public CompletableFuture<List<Entity>> get(DataFetchingEnvironment environment) {
    final List<Entity> entities = _entitiesProvider.apply(environment);
    return BatchLoadUtils.batchLoadEntitiesOfSameType(
        entities, _entityTypes, environment.getDataLoaderRegistry());
  }
}
