package com.linkedin.datahub.graphql.resolvers.load;

import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.BatchLoadUtils;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BatchGetEntitiesResolver implements DataFetcher<CompletableFuture<List<Entity>>> {

  private final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> _entityTypes;
  private final Function<DataFetchingEnvironment, List<Entity>> _entitiesProvider;

  public BatchGetEntitiesResolver(
      final List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
      final Function<DataFetchingEnvironment, List<Entity>> entitiesProvider) {
    _entityTypes = entityTypes;
    _entitiesProvider = entitiesProvider;
  }

  @Override
  public CompletableFuture<List<Entity>> get(DataFetchingEnvironment environment) {
    final List<Entity> entities = _entitiesProvider.apply(environment);
    Map<EntityType, List<Entity>> entityTypeToEntities = new HashMap<>();

    Map<String, List<Integer>> entityIndexMap = new HashMap<>();
    int index = 0;
    for (Entity entity : entities) {
      List<Integer> indexList = new ArrayList<>();
      if (entityIndexMap.containsKey(entity.getUrn())) {
        indexList = entityIndexMap.get(entity.getUrn());
      }
      indexList.add(index);
      entityIndexMap.put(entity.getUrn(), indexList);
      index++;
      EntityType type = entity.getType();
      List<Entity> entitiesList = entityTypeToEntities.getOrDefault(type, new ArrayList<>());
      entitiesList.add(entity);
      entityTypeToEntities.put(type, entitiesList);
    }

    List<CompletableFuture<List<Entity>>> entitiesFutures = new ArrayList<>();

    for (Map.Entry<EntityType, List<Entity>> entry : entityTypeToEntities.entrySet()) {
      CompletableFuture<List<Entity>> entitiesFuture =
          BatchLoadUtils.batchLoadEntitiesOfSameType(
              entry.getValue(), _entityTypes, environment.getDataLoaderRegistry());
      entitiesFutures.add(entitiesFuture);
    }

    return CompletableFuture.allOf(entitiesFutures.toArray(new CompletableFuture[0]))
        .thenApply(
            v -> {
              Entity[] finalEntityList = new Entity[entities.size()];
              // Returned objects can be either of type Entity or wrapped as
              // DataFetcherResult<Entity>
              // Therefore we need to be working with raw Objects in this area of the code
              List<Object> returnedList =
                  entitiesFutures.stream()
                      .flatMap(future -> future.join().stream())
                      .collect(Collectors.toList());
              for (Object element : returnedList) {
                Entity entity = null;
                if (element instanceof DataFetcherResult) {
                  entity = ((DataFetcherResult<Entity>) element).getData();
                } else if (element instanceof Entity) {
                  entity = (Entity) element;
                } else {
                  throw new RuntimeException(
                      String.format(
                          "Cannot process entity because it is neither an Entity not a DataFetcherResult. %s",
                          element));
                }
                for (int idx : entityIndexMap.get(entity.getUrn())) {
                  finalEntityList[idx] = entity;
                }
              }
              return Arrays.asList(finalEntityList);
            });
  }
}
