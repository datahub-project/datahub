package com.linkedin.datahub.graphql.resolvers.load;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.OwnerType;
import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

/**
 * GraphQL resolver responsible for
 *
 * <p>1. Retrieving a batch of OwnerTypes. 2. Resolving their Entities
 */
public class OwnerTypeBatchResolver implements DataFetcher<CompletableFuture<List<OwnerType>>> {

  private final List<com.linkedin.datahub.graphql.types.LoadableType<?, ?>> _loadableTypes;
  private final Function<DataFetchingEnvironment, List<OwnerType>> _typesProvider;

  public OwnerTypeBatchResolver(
      final List<com.linkedin.datahub.graphql.types.LoadableType<?, ?>> loadableTypes,
      final Function<DataFetchingEnvironment, List<OwnerType>> typesProvider) {
    _loadableTypes = loadableTypes;
    _typesProvider = typesProvider;
  }

  @Override
  public CompletableFuture<List<OwnerType>> get(DataFetchingEnvironment environment) {
    // 1. get OwnerTypes
    final List<OwnerType> ownerTypes = _typesProvider.apply(environment);
    if (ownerTypes == null || ownerTypes.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    // 2. Filter and group OwnerTypes by entity types
    final Map<String, List<OwnerType>> filteredEntitiesMap = new HashMap<>();
    for (final OwnerType ownerType : ownerTypes) {
      final LoadableType<?, ?> filteredEntity =
          Iterables.getOnlyElement(
              _loadableTypes.stream()
                  .filter(entity -> ownerType.getClass().isAssignableFrom(entity.objectClass()))
                  .collect(Collectors.toList()),
              null);
      if (filteredEntity == null) {
        continue;
      }
      filteredEntitiesMap.putIfAbsent(filteredEntity.name(), new ArrayList<>());
      filteredEntitiesMap.get(filteredEntity.name()).add(ownerType);
    }

    // 3. Generate batch load requests for each EntityType
    final List<CompletableFuture<List<OwnerType>>> batchLoadFutures =
        filteredEntitiesMap.entrySet().stream()
            .map(
                (set) ->
                    loadUniformTypes(
                        environment.getDataLoaderRegistry(), set.getKey(), set.getValue()))
            .toList();

    // 4. Flatmap the results of the futures
    return CompletableFuture.allOf(batchLoadFutures.toArray(new CompletableFuture[0]))
        .thenApply(
            v ->
                batchLoadFutures.stream()
                    .map(CompletableFuture::join)
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
  }

  private CompletableFuture<List<OwnerType>> loadUniformTypes(
      DataLoaderRegistry dataLoaderRegistry,
      String filteredEntityName,
      List<OwnerType> ownerTypes) {
    final DataLoader<Object, OwnerType> loader =
        dataLoaderRegistry.getDataLoader(filteredEntityName);
    final List<Object> keyList =
        ownerTypes.stream().map(ownerType -> (Object) ((Entity) ownerType).getUrn()).toList();
    return loader.loadMany(keyList);
  }
}
