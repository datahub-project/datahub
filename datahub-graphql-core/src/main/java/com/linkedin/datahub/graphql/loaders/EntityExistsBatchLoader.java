package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.metadata.entity.EntityService;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader for the {@code exists} field. Resolves every urn in a request with one
 * read instead of one read per entity.
 *
 * <p>Unlike the hierarchy loaders this one does not use {@code Try}. The whole batch is a single
 * read, so there is no per-key work that could fail on its own — if the read fails, every key in
 * the batch is unresolved anyway.
 */
public final class EntityExistsBatchLoader {

  public static final String LOADER_NAME = "EntityExists";

  private EntityExistsBatchLoader() {}

  public static DataLoader<Urn, Boolean> create(
      final EntityService<?> entityService, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Keep the batchLoad span under the operation rather than the executor thread.
    final Context batchContext = Context.current();

    return DataLoader.newDataLoader(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try (Scope ignored = batchContext.makeCurrent()) {
                    return batchLoad(keys, (QueryContext) env.getContext(), entityService);
                  }
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public static List<Boolean> batchLoad(
      final List<Urn> urns, final QueryContext queryContext, final EntityService<?> entityService) {

    // This overload counts soft-deleted entities as existing.
    final Set<Urn> distinct = new LinkedHashSet<>(urns);
    final Set<Urn> existing;
    try {
      existing = entityService.exists(queryContext.getOperationContext(), distinct);
    } catch (Exception e) {
      // Throw rather than return false, which would look like a deleted entity.
      throw new RuntimeException(
          String.format("Failed to check whether %d entities exist", distinct.size()), e);
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    final List<Boolean> ordered = new ArrayList<>(urns.size());
    for (Urn urn : urns) {
      ordered.add(existing.contains(urn));
    }
    return ordered;
  }
}
