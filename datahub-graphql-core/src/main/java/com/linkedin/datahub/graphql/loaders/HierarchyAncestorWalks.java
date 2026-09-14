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
   * Walks every entity's ancestor chain concurrently, completing when all of them are done.
   *
   * <p>Returns a future instead of blocking. These walks run on the same executor the caller is
   * already running on, so waiting for them here would hold a pool thread until tasks queued behind
   * it finish.
   *
   * <p>Every key is present in the result. A walk that failed is recorded as a failed {@link Try}
   * against its own key, so one bad entity does not fail the batch.
   */
  static CompletableFuture<Map<Urn, Try<List<Urn>>>> resolveConcurrently(
      final List<Urn> urns,
      final OperationContext opContext,
      final HierarchyReadSpec spec,
      final int maxDepth,
      final String loaderName) {

    final Map<Urn, CompletableFuture<Try<List<Urn>>>> futures = new LinkedHashMap<>(urns.size());
    for (Urn urn : urns) {
      futures.put(
          urn,
          GraphQLConcurrencyUtils.supplyAsync(
                  () -> BoundHierarchyAccess.orderedParents(opContext, spec, urn, maxDepth),
                  loaderName,
                  "orderedParents")
              // Turn a failure into a successful future holding a failed Try, so the allOf below
              // never short-circuits on the first one.
              .handle(
                  (parents, error) ->
                      error == null ? Try.succeeded(parents) : Try.failed(unwrap(error))));
    }

    return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
        .thenApply(
            ignored -> {
              final Map<Urn, Try<List<Urn>>> ancestors = new LinkedHashMap<>(futures.size());
              // Every future is already complete here, so join returns without waiting.
              futures.forEach((urn, future) -> ancestors.put(urn, future.join()));
              return ancestors;
            });
  }

  /** Unwrap the CompletionException so the caller sees the real cause. */
  private static Throwable unwrap(final Throwable t) {
    return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
  }
}
