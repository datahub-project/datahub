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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
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
    List<List<Entity>> inputEntitiesPerFuture = new ArrayList<>();

    for (Map.Entry<EntityType, List<Entity>> entry : entityTypeToEntities.entrySet()) {
      CompletableFuture<List<Entity>> entitiesFuture =
          BatchLoadUtils.batchLoadEntitiesOfSameType(
              entry.getValue(), _entityTypes, environment.getDataLoaderRegistry());
      entitiesFutures.add(entitiesFuture);
      inputEntitiesPerFuture.add(entry.getValue());
    }

    return CompletableFuture.allOf(entitiesFutures.toArray(new CompletableFuture[0]))
        .thenApply(
            v -> {
              Entity[] finalEntityList = new Entity[entities.size()];

              // Process each batch of results with corresponding input entities
              Iterator<List<Entity>> inputIterator = inputEntitiesPerFuture.iterator();
              for (CompletableFuture<List<Entity>> future : entitiesFutures) {
                List<Entity> inputEntities = inputIterator.next();
                List<Object> loadedEntities = (List<Object>) (List<?>) future.join();

                // DataLoader preserves order, so match by position
                Iterator<Entity> inputEntityIterator = inputEntities.iterator();
                for (Object element : loadedEntities) {
                  Entity entity = null;
                  if (element == null) {
                    // Entity doesn't exist, leave as null
                  } else if (element instanceof DataFetcherResult) {
                    entity = ((DataFetcherResult<Entity>) element).getData();
                  } else if (element instanceof Entity) {
                    entity = (Entity) element;
                  } else {
                    throw new RuntimeException(
                        String.format(
                            "Cannot process entity because it is neither an Entity nor a DataFetcherResult. %s",
                            element));
                  }

                  // Use INPUT entity's URN for lookup (loaded entity may have encrypted URN)
                  String urn = inputEntityIterator.next().getUrn();
                  for (int idx : entityIndexMap.get(urn)) {
                    finalEntityList[idx] = entity;
                  }
                }
              }
              return Arrays.asList(finalEntityList);
            });
  }
}
