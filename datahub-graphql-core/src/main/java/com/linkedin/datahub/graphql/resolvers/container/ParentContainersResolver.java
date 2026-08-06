package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.types.container.mappers.ContainerMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ParentContainersResolver
    implements DataFetcher<CompletableFuture<ParentContainersResult>> {

  private final EntityClient _entityClient;

  public ParentContainersResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ParentContainersResult> get(DataFetchingEnvironment environment) {

    final QueryContext context = getQueryContext(environment);
    final Urn urn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> parentUrns =
                BoundHierarchyAccess.orderedParents(
                    context.getOperationContext(),
                    HierarchyBindings.containerSpec(context.getOperationContext()),
                    urn,
                    context.getMaxParentDepth());

            List<Container> containers = new ArrayList<>();
            if (!parentUrns.isEmpty()) {
              // All ancestors in a container hierarchy are containers, so a single batch call
              // over one entity type replaces the per-parent getV2 round-trips (N+1).
              Map<Urn, EntityResponse> responses =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      parentUrns.get(0).getEntityType(),
                      new HashSet<>(parentUrns),
                      null);

              // batchGetV2 returns an unordered map; re-iterate parentUrns to preserve hierarchy
              // order. Missing/unauthorized urns resolve to null and are skipped, matching the
              // previous response != null behavior.
              for (Urn parentUrn : parentUrns) {
                EntityResponse response = responses.get(parentUrn);
                if (response != null) {
                  containers.add(ContainerMapper.map(context, response));
                }
              }
            }

            final ParentContainersResult result = new ParentContainersResult();
            result.setCount(containers.size());
            result.setContainers(containers);
            return result;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to load parent containers for entity %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
