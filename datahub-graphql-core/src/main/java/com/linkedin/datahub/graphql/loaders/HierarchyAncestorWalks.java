package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyReadSpec;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.dataloader.Try;

/** Shared ancestor-walk step for the hierarchy batch loaders. */
final class HierarchyAncestorWalks {

  private HierarchyAncestorWalks() {}

  /**
   * Walks every entity's ancestor chain as one batch, in a single task.
   *
   * <p>One task per entity used to be submitted here, with no bound. A page of 100 entities then
   * ran 100 walks at once, each borrowing a DB connection per ancestor level, which could exhaust
   * the connection pool and grow the shared executor to its thread limit. The batch walk reads each
   * level once for every entity (see {@link BoundHierarchyAccess#orderedParentsBatch}).
   *
   * <p>Returns a future instead of blocking, so the caller's pool thread is not held while the walk
   * runs.
   *
   * <p>Every key is present in the result. A walk that failed is recorded as a failed {@link Try}
   * against the keys it affected, so one bad entity does not fail the batch.
   */
  static CompletableFuture<Map<Urn, Try<List<Urn>>>> resolveBatch(
      final List<Urn> urns,
      final OperationContext opContext,
      final HierarchyReadSpec spec,
      final int maxDepth,
      final String loaderName) {

    return GraphQLConcurrencyUtils.supplyAsync(
            () -> BoundHierarchyAccess.orderedParentsBatch(opContext, spec, urns, maxDepth),
            loaderName,
            "orderedParentsBatch")
        .handle(
            (batch, error) -> {
              final Map<Urn, Try<List<Urn>>> ancestors = new LinkedHashMap<>(urns.size());
              for (Urn urn : urns) {
                if (error != null) {
                  ancestors.put(urn, Try.failed(unwrap(error)));
                } else if (batch.getFailures().containsKey(urn)) {
                  ancestors.put(urn, Try.failed(batch.getFailures().get(urn)));
                } else if (batch.getParents().containsKey(urn)) {
                  ancestors.put(urn, Try.succeeded(batch.getParents().get(urn)));
                } else {
                  ancestors.put(
                      urn, Try.failed(new IllegalStateException("no ancestor walk for " + urn)));
                }
              }
              return ancestors;
            });
  }

  /** Unwrap the CompletionException so the caller sees the real cause. */
  private static Throwable unwrap(final Throwable t) {
    return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
  }
}
