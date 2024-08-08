package com.linkedin.datahub.graphql.resolvers;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.generated.Entity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

public class BatchLoadUtils {

  private BatchLoadUtils() {}

  public static CompletableFuture<List<Entity>> batchLoadEntitiesOfSameType(
      List<Entity> entities,
      List<com.linkedin.datahub.graphql.types.EntityType<?, ?>> entityTypes,
      DataLoaderRegistry dataLoaderRegistry) {
    if (entities.isEmpty()) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }
    // Assume all entities are of the same type
    final com.linkedin.datahub.graphql.types.EntityType filteredEntity =
        Iterables.getOnlyElement(
            entityTypes.stream()
                .filter(entity -> entities.get(0).getClass().isAssignableFrom(entity.objectClass()))
                .collect(Collectors.toList()));

    final DataLoader<Object, Entity> loader =
        dataLoaderRegistry.getDataLoader(filteredEntity.name());
    List<Object> keyList = new ArrayList();
    for (Entity entity : entities) {
      keyList.add(filteredEntity.getKeyProvider().apply(entity));
    }
    return loader.loadMany(keyList);
  }
}
