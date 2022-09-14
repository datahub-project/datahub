package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.BatchLoadUtils;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BatchGetEntitiesResolver implements DataFetcher<CompletableFuture<List<Entity>>> {

  private final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> _entityTypes;
  private final Function<DataFetchingEnvironment, List<Entity>> _entitiesProvider;

  public BatchGetEntitiesResolver(
      final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
      final Function<DataFetchingEnvironment, List<Entity>> entitiesProvider
  ) {
    _entityTypes = entityTypes;
    _entitiesProvider = entitiesProvider;
  }

  @Override
  public CompletableFuture<List<Entity>> get(DataFetchingEnvironment environment) {
    final List<Entity> entities = _entitiesProvider.apply(environment);
    Map<EntityType, List<Entity>> entityTypeToEntities = new HashMap<>();

    entities.forEach((entity) -> {
      EntityType type = entity.getType();
      List<Entity> entitiesList = entityTypeToEntities.getOrDefault(type, new ArrayList<>());
      entitiesList.add(entity);
      entityTypeToEntities.put(type, entitiesList);
    });

    List<CompletableFuture<List<Entity>>> entitiesFutures = new ArrayList<>();

    for (Map.Entry<EntityType, List<Entity>> entry : entityTypeToEntities.entrySet()) {
      CompletableFuture<List<Entity>> entitiesFuture = BatchLoadUtils
          .batchLoadEntitiesOfSameType(entry.getValue(), _entityTypes, environment.getDataLoaderRegistry());
      entitiesFutures.add(entitiesFuture);
    }

    return CompletableFuture.allOf(entitiesFutures.toArray(new CompletableFuture[0]))
        .thenApply(v -> entitiesFutures.stream().flatMap(future -> future.join().stream()).collect(Collectors.toList()));
  }
}
