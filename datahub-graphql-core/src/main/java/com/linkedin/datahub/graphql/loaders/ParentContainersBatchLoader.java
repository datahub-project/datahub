package com.linkedin.datahub.graphql.loaders;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.types.container.mappers.ContainerMapper;
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
import org.dataloader.DataLoaderOptions;

/**
 * Per-request DataLoader for {@code parentContainers}. Fetches the ancestors of every entity in a
 * request with one call instead of one call per entity.
 */
@Slf4j
public final class ParentContainersBatchLoader {

  public static final String LOADER_NAME = "ParentContainers";

  private ParentContainersBatchLoader() {}

  public static DataLoader<Urn, ParentContainersResult> create(
      final EntityClient entityClient, final QueryContext queryContext) {
    final BatchLoaderContextProvider provider = () -> queryContext;
    final DataLoaderOptions options =
        DataLoaderOptions.newOptions().setBatchLoaderContextProvider(provider);

    // Parent the batchLoad span under the operation, not the executor thread (see
    // GmsGraphQLEngine#createDataLoader).
    final Context batchContext = Context.current();

    return DataLoader.newDataLoader(
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
  public static List<ParentContainersResult> batchLoad(
      final List<Urn> urns, final QueryContext queryContext, final EntityClient entityClient) {

    final List<Urn> distinct = urns.stream().distinct().collect(Collectors.toList());
    final Map<Urn, List<Urn>> ancestorsByUrn = resolveAncestors(distinct, queryContext);
    return assemble(urns, distinct, queryContext, entityClient, ancestorsByUrn);
  }

  /**
   * The half after the ancestor walk, with the chains supplied. Lets the fetch-and-assemble
   * behaviour be tested without a live hierarchy cache.
   */
  static List<ParentContainersResult> batchLoadForTest(
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

  private static List<ParentContainersResult> assemble(
      final List<Urn> urns,
      final List<Urn> distinct,
      final QueryContext queryContext,
      final EntityClient entityClient,
      final Map<Urn, List<Urn>> ancestorsByUrn) {

    final Set<Urn> allAncestors =
        ancestorsByUrn.values().stream().flatMap(List::stream).collect(Collectors.toSet());
    final Map<Urn, EntityResponse> responses =
        fetchContainers(allAncestors, queryContext, entityClient);

    final Map<Urn, ParentContainersResult> resultByUrn = new HashMap<>(distinct.size());
    for (Urn urn : distinct) {
      final List<Container> containers = new ArrayList<>();
      // Re-iterate the ancestor list to keep hierarchy order; a missing or unauthorized ancestor is
      // skipped, matching the unbatched resolver.
      for (Urn ancestor : ancestorsByUrn.getOrDefault(urn, List.of())) {
        final EntityResponse response = responses.get(ancestor);
        if (response != null) {
          containers.add(ContainerMapper.map(queryContext, response));
        }
      }
      final ParentContainersResult result = new ParentContainersResult();
      result.setCount(containers.size());
      result.setContainers(containers);
      resultByUrn.put(urn, result);
    }

    // DataLoader contract: results[i] must correspond to keys[i].
    return urns.stream().map(resultByUrn::get).collect(Collectors.toList());
  }

  /**
   * Walks each entity's ancestor chain. A cache hit still costs a Hazelcast read, and a miss reads
   * one aspect per level, so the walks run concurrently rather than in a loop — batching that
   * serialised them would cost more than it saves.
   */
  private static Map<Urn, List<Urn>> resolveAncestors(
      final List<Urn> urns, final QueryContext queryContext) {

    final OperationContext opContext = queryContext.getOperationContext();
    final HierarchyReadSpec spec = HierarchyBindings.containerSpec(opContext);
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

  /** Ancestors of a container hierarchy are containers, but group by type so this stays correct. */
  private static Map<Urn, EntityResponse> fetchContainers(
      final Set<Urn> ancestors, final QueryContext queryContext, final EntityClient entityClient) {

    if (ancestors.isEmpty()) {
      return Map.of();
    }
    final Map<Urn, EntityResponse> responses = new HashMap<>(ancestors.size());
    final Map<String, Set<Urn>> byEntityType =
        ancestors.stream()
            .collect(
                Collectors.groupingBy(Urn::getEntityType, Collectors.toCollection(HashSet::new)));
    try {
      for (Map.Entry<String, Set<Urn>> group : byEntityType.entrySet()) {
        responses.putAll(
            entityClient.batchGetV2(
                queryContext.getOperationContext(), group.getKey(), group.getValue(), null));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load parent containers", e);
    }
    return responses;
  }
}
