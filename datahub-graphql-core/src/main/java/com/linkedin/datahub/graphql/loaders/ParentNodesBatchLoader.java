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
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import com.linkedin.metadata.graph.cache.client.HierarchyReadSpec;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderFactory;
import org.dataloader.DataLoaderOptions;
import org.dataloader.Try;

/**
 * Per-request DataLoader for {@code parentNodes}. Fetches the ancestors of every glossary entity in
 * a request with one call instead of one call per entity.
 */
@Slf4j
public final class ParentNodesBatchLoader {

  public static final String LOADER_NAME = "ParentNodes";

  private ParentNodesBatchLoader() {}

  public static DataLoader<Urn, ParentNodesResult> create(
      final EntityClient entityClient, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Parent the batchLoad span under the operation, not the executor thread (see
    // GmsGraphQLEngine#createDataLoader).
    final Context batchContext = Context.current();

    // withTry so one entity's unresolvable ancestor fails only that field, as it did before.
    return DataLoaderFactory.newDataLoaderWithTry(
        (keys, env) ->
            GraphQLConcurrencyUtils.supplyAsync(
                () -> {
                  try (Scope ignored = batchContext.makeCurrent()) {
                    return batchLoad(keys, (QueryContext) env.getContext(), entityClient);
                  }
                },
                LOADER_NAME,
                "batchLoad"),
        options);
  }

  @WithSpan
  public static List<Try<ParentNodesResult>> batchLoad(
      final List<Urn> urns, final QueryContext queryContext, final EntityClient entityClient) {

    final List<Urn> distinct = urns.stream().distinct().collect(Collectors.toList());
    final Map<Urn, List<Urn>> ancestorsByUrn = resolveAncestors(distinct, queryContext);
    return assemble(urns, distinct, queryContext, entityClient, ancestorsByUrn);
  }

  /**
   * The half after the ancestor walk, with the chains supplied. Lets the fetch-and-assemble
   * behaviour be tested without a live hierarchy cache.
   */
  static List<Try<ParentNodesResult>> batchLoadForTest(
      final List<Urn> urns,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final Map<Urn, List<Urn>> ancestorsByUrn) {
    return assemble(
        urns,
        urns.stream().distinct().collect(Collectors.toList()),
        queryContext,
        entityClient,
        ancestorsByUrn);
  }

  private static List<Try<ParentNodesResult>> assemble(
      final List<Urn> urns,
      final List<Urn> distinct,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final Map<Urn, List<Urn>> ancestorsByUrn) {

    final Set<Urn> allAncestors =
        ancestorsByUrn.values().stream().flatMap(List::stream).collect(Collectors.toSet());

    final Map<Urn, EntityResponse> responses;
    try {
      responses = fetchNodes(allAncestors, queryContext, entityClient);
    } catch (Exception e) {
      // A failed fetch is shared by every key in the batch.
      final Try<ParentNodesResult> failure = Try.failed(e);
      return urns.stream().map(urn -> failure).collect(Collectors.toList());
    }

    final Map<Urn, Try<ParentNodesResult>> resultByUrn = new HashMap<>(distinct.size());
    for (Urn urn : distinct) {
      resultByUrn.put(urn, resolveOne(urn, ancestorsByUrn, responses, queryContext));
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    return urns.stream().map(resultByUrn::get).collect(Collectors.toList());
  }

  private static Try<ParentNodesResult> resolveOne(
      final Urn sourceUrn,
      final Map<Urn, List<Urn>> ancestorsByUrn,
      final Map<Urn, EntityResponse> responses,
      final QueryContext queryContext) {

    final List<GlossaryNode> viewable = new ArrayList<>();
    // Re-iterate the ancestor list to keep hierarchy order; visibility is evaluated per source, so
    // it cannot be shared across keys even though the fetch is.
    for (Urn ancestor : ancestorsByUrn.getOrDefault(sourceUrn, List.of())) {
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

  /**
   * Walks each entity's ancestor chain. A cache hit still costs a Hazelcast read, and a miss reads
   * one aspect per level, so the walks run concurrently rather than in a loop.
   */
  private static Map<Urn, List<Urn>> resolveAncestors(
      final List<Urn> urns, final QueryContext queryContext) {

    final OperationContext opContext = queryContext.getOperationContext();
    final HierarchyReadSpec spec = HierarchyBindings.glossarySpec(opContext);
    final int maxDepth = queryContext.getMaxParentDepth();

    final Map<Urn, CompletableFuture<List<Urn>>> futures = new LinkedHashMap<>(urns.size());
    for (Urn urn : urns) {
      futures.put(
          urn,
          GraphQLConcurrencyUtils.supplyAsync(
              () -> BoundHierarchyAccess.orderedParents(opContext, spec, urn, maxDepth),
              LOADER_NAME,
              "orderedParents"));
    }
    CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).join();

    final Map<Urn, List<Urn>> ancestors = new LinkedHashMap<>(urns.size());
    futures.forEach((urn, future) -> ancestors.put(urn, future.join()));
    return ancestors;
  }

  /** Ancestors of a glossary hierarchy are nodes, but group by type so this stays correct. */
  private static Map<Urn, EntityResponse> fetchNodes(
      final Set<Urn> ancestors, final QueryContext queryContext, final EntityClient entityClient)
      throws Exception {

    if (ancestors.isEmpty()) {
      return Map.of();
    }
    final Map<Urn, EntityResponse> responses = new HashMap<>(ancestors.size());
    final Map<String, Set<Urn>> byEntityType =
        ancestors.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));
    for (Map.Entry<String, Set<Urn>> group : byEntityType.entrySet()) {
      responses.putAll(
          entityClient.batchGetV2(
              queryContext.getOperationContext(), group.getKey(), group.getValue(), null));
    }
    return responses;
  }
}
