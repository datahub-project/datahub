package com.linkedin.datahub.graphql.resolvers;

import com.google.common.collect.Iterables;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.QueryContext;
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
      DataLoaderRegistry dataLoaderRegistry,
      QueryContext context) {
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

    // This opaque batch path (entities(urns:), browse, autocomplete, siblings, EntityPath,
    // structured-property valueEntities) loads by key without a per-field selection, so it cannot
    // compute its own aspect requirements. Contribute FETCH_ALL to the request-scoped union so the
    // batchLoad does not reuse a narrower AspectLoadContext left by an earlier typed selection in
    // the same request and under-hydrate. Widening to fetch-all is safe (over-fetch, never under).
    List<Object> keyList = new ArrayList();
    for (Entity entity : entities) {
      keyList.add(filteredEntity.getKeyProvider().apply(entity));
    }
    if (context != null) {
      context.mergeAspectLoadContext(filteredEntity.name(), AspectLoadContext.fetchAll());
      // FETCH_ALL must also ride along as the DataLoader key context: loads without one use the
      // legacy key-only cache key, so a prior dispatch of the same URN under a narrower union
      // would be served from cache and skip batchLoad entirely, undoing the widening above.
      return loader.loadMany(
          keyList, Collections.nCopies(keyList.size(), AspectLoadContext.fetchAll()));
    }
    return loader.loadMany(keyList);
  }
}
