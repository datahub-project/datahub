package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentContainersResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ParentContainersResolver
    implements DataFetcher<CompletableFuture<ParentContainersResult>> {

  @Override
  public CompletableFuture<ParentContainersResult> get(DataFetchingEnvironment environment) {

    final QueryContext context = getQueryContext(environment);
    final Urn urn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> containerUrns =
                BoundHierarchyAccess.orderedParents(
                    context.getOperationContext(),
                    HierarchyBindings.containerSpec(context.getOperationContext()),
                    urn,
                    context.getMaxParentDepth());

            return ParentContainersResult.builder()
                .setCount(containerUrns.size())
                .setContainers(
                    containerUrns.stream()
                        .map(it -> (Container) UrnToEntityMapper.map(context, it))
                        .toList())
                .build();
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to load parent containers for entity %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
