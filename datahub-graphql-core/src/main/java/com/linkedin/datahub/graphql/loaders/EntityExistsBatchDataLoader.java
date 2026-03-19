package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.metadata.entity.EntityService;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * DataLoader for batching multiple entity existence checks into a single database query.
 *
 * <p>Converts N+1 queries (one exists() call per entity) into a single batched call. Example: 100
 * entities requesting exists field → 1 batched query instead of 100 individual queries.
 */
public class EntityExistsBatchDataLoader {

  private EntityExistsBatchDataLoader() {}

  /**
   * Creates a DataLoader for batch loading entity existence status.
   *
   * @param entityService the entity service to use for existence checks
   * @param queryContext the query context
   * @return a DataLoader that batches URN existence checks
   */
  public static DataLoader<String, Boolean> create(
      final EntityService<?> entityService, final QueryContext queryContext) {
    BatchLoaderContextProvider contextProvider = () -> queryContext;
    DataLoaderOptions loaderOptions =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
    return DataLoader.newDataLoader(
        (keys, context) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> batchLoad(keys, entityService, context.getContext()),
                EntityExistsBatchDataLoader.class.getSimpleName(),
                "batchLoad"),
        loaderOptions);
  }

  /**
   * Batch loads existence status for multiple URNs.
   *
   * @param urnStrs list of URN strings to check
   * @param entityService the entity service
   * @param queryContext the query context
   * @return list of booleans indicating existence status, in same order as input
   */
  private static List<Boolean> batchLoad(
      final List<String> urnStrs,
      final EntityService<?> entityService,
      final QueryContext queryContext) {
    try {
      // Convert string URNs to Urn objects (parse once, maintain order)
      final List<Urn> parsedUrns = new ArrayList<>();
      final Set<Urn> urnSet = new HashSet<>();
      for (final String urnStr : urnStrs) {
        Urn urn = Urn.createFromString(urnStr);
        parsedUrns.add(urn);
        urnSet.add(urn);
      }

      // Batch load existence status for all URNs in one call
      final Set<Urn> existingUrns =
          entityService.exists(queryContext.getOperationContext(), urnSet);

      // Map results back to original order using ordered list
      final List<Boolean> results = new ArrayList<>();
      for (final Urn urn : parsedUrns) {
        results.add(existingUrns.contains(urn));
      }

      return results;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to batch load entity existence for urns: %s", urnStrs), e);
    }
  }
}
