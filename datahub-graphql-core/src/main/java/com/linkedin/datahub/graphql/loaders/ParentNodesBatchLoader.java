package com.linkedin.datahub.graphql.loaders;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryNodeMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderFactory;
import org.dataloader.DataLoaderOptions;
import org.dataloader.Try;

/**
 * Per-request DataLoader for {@code parentNodes}. Fetches the ancestors of every glossary entity in
 * a request with one call instead of one call per entity.
 */
public final class ParentNodesBatchLoader {

  public static final String LOADER_NAME = "ParentNodes";

  private ParentNodesBatchLoader() {}

  public static DataLoader<Urn, ParentNodesResult> create(
      final EntityClient entityClient, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Keep the batchLoad span under the operation rather than the executor thread.
    final Context batchContext = Context.current();

    // withTry so one entity's failure fails only its own field.
    return DataLoaderFactory.newDataLoaderWithTry(
        (keys, env) -> {
          try (Scope ignored = batchContext.makeCurrent()) {
            return batchLoad(keys, (QueryContext) env.getContext(), entityClient);
          }
        },
        options);
  }

  @WithSpan
  public static CompletableFuture<List<Try<ParentNodesResult>>> batchLoad(
      final List<Urn> urns, final QueryContext queryContext, final EntityClient entityClient) {

    final List<Urn> distinct = urns.stream().distinct().collect(Collectors.toList());
    // Captured while the caller's scope is still open; the continuation below runs on a different
    // thread once the walks finish.
    final Context batchContext = Context.current();

    return resolveAncestors(distinct, queryContext)
        .thenCompose(
            chains ->
                // Submit the hydration as its own task rather than running it on whichever thread
                // finished the last walk, since it does blocking I/O.
                GraphQLConcurrencyUtils.supplyAsync(
                    () -> {
                      try (Scope ignored = batchContext.makeCurrent()) {
                        return assemble(urns, distinct, queryContext, entityClient, chains);
                      }
                    },
                    LOADER_NAME,
                    "assemble"));
  }

  /** Fetch and assemble, given each entity's already-resolved ancestor chain. */
  static List<Try<ParentNodesResult>> assemble(
      final List<Urn> urns,
      final List<Urn> distinct,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final Map<Urn, Try<List<Urn>>> ancestorsByUrn) {

    // Only successful chains contribute ancestors.
    final Set<Urn> allAncestors =
        ancestorsByUrn.values().stream()
            .filter(Try::isSuccess)
            .map(Try::get)
            .flatMap(List::stream)
            .collect(Collectors.toSet());

    final Map<String, Set<Urn>> byEntityType =
        allAncestors.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));

    Map<Urn, EntityResponse> responses = Map.of();
    Throwable fetchFailure = null;
    try {
      responses = fetchNodes(byEntityType, queryContext, entityClient);
    } catch (Exception e) {
      fetchFailure = e;
    }

    final Map<Urn, Try<ParentNodesResult>> resultByUrn = new HashMap<>(distinct.size());
    for (Urn urn : distinct) {
      // A failed walk arrives as a failed Try, so a missing entry is not a walk failure — it means
      // the caller passed an incomplete map. Fail loudly rather than report it as this urn's error.
      final Try<List<Urn>> chain =
          Objects.requireNonNull(ancestorsByUrn.get(urn), () -> "no ancestor walk for " + urn);
      if (chain.isFailure()) {
        resultByUrn.put(urn, Try.failed(chain.getThrowable()));
        continue;
      }
      if (fetchFailure != null && !chain.get().isEmpty()) {
        // Only keys that needed the fetch are affected by its failure.
        resultByUrn.put(urn, Try.failed(fetchFailure));
        continue;
      }
      try {
        resultByUrn.put(urn, resolveOne(urn, chain.get(), responses, queryContext));
      } catch (Exception e) {
        // Keep a mapping or visibility failure off the other keys.
        resultByUrn.put(urn, Try.failed(e));
      }
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    return urns.stream()
        .map(
            urn ->
                Objects.requireNonNull(
                    resultByUrn.get(urn), () -> "no batch result produced for " + urn))
        .collect(Collectors.toList());
  }

  private static Try<ParentNodesResult> resolveOne(
      final Urn sourceUrn,
      final List<Urn> ancestors,
      final Map<Urn, EntityResponse> responses,
      final QueryContext queryContext) {

    final List<GlossaryNode> viewable = new ArrayList<>();
    // Iterate the chain to keep hierarchy order. Visibility depends on the source, so it is
    // checked per key even though the fetch is shared.
    for (Urn ancestor : ancestors) {
      final EntityResponse response = responses.get(ancestor);
      if (response == null) {
        return Try.failed(new RuntimeException("Failed to retrieve glossary node " + ancestor));
      }
      final GlossaryNode node = GlossaryNodeMapper.map(queryContext, response);
      if (canViewRelationship(
          queryContext.getOperationContext(), UrnUtils.getUrn(node.getUrn()), sourceUrn)) {
        viewable.add(node);
      }
    }

    final ParentNodesResult result = new ParentNodesResult();
    result.setCount(viewable.size());
    result.setNodes(viewable);
    return Try.succeeded(result);
  }

  private static CompletableFuture<Map<Urn, Try<List<Urn>>>> resolveAncestors(
      final List<Urn> urns, final QueryContext queryContext) {
    final OperationContext opContext = queryContext.getOperationContext();
    return HierarchyAncestorWalks.resolveConcurrently(
        urns,
        opContext,
        HierarchyBindings.glossarySpec(opContext),
        queryContext.getMaxParentDepth(),
        LOADER_NAME);
  }

  /**
   * Ancestors are glossary nodes, but group by type so a mixed chain still works. The unbatched
   * resolver assumed one type and sent every ancestor under the first one's, which fails a mixed
   * chain. Chains are homogeneous today, so this only matters if that changes.
   */
  private static Map<Urn, EntityResponse> fetchNodes(
      final Map<String, Set<Urn>> byEntityType,
      final QueryContext queryContext,
      final EntityClient entityClient)
      throws Exception {

    final Map<Urn, EntityResponse> responses = new HashMap<>();
    for (Map.Entry<String, Set<Urn>> group : byEntityType.entrySet()) {
      responses.putAll(
          entityClient.batchGetV2(
              queryContext.getOperationContext(), group.getKey(), group.getValue(), null));
    }
    return responses;
  }
}
