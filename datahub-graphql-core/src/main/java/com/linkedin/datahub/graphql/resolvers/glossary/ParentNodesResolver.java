package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.ParentNodesResult;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryNodeMapper;
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

public class ParentNodesResolver implements DataFetcher<CompletableFuture<ParentNodesResult>> {

  private final EntityClient _entityClient;

  public ParentNodesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ParentNodesResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final Urn sourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> parentUrns =
                BoundHierarchyAccess.orderedParents(
                    context.getOperationContext(),
                    HierarchyBindings.glossarySpec(context.getOperationContext()),
                    sourceUrn,
                    context.getMaxParentDepth());

            List<GlossaryNode> viewable = new ArrayList<>();
            if (!parentUrns.isEmpty()) {
              // All ancestors in a glossary hierarchy are glossary nodes, so a single batch call
              // over one entity type replaces the per-parent getV2 round-trips (N+1).
              Map<Urn, EntityResponse> responses =
                  _entityClient.batchGetV2(
                      context.getOperationContext(),
                      parentUrns.get(0).getEntityType(),
                      new HashSet<>(parentUrns),
                      null);

              // Re-iterate parentUrns to preserve hierarchy order (batchGetV2 returns an unordered
              // map), then apply the same relationship-visibility filter as before.
              for (Urn parentUrn : parentUrns) {
                EntityResponse response = responses.get(parentUrn);
                if (response == null) {
                  throw new RuntimeException("Failed to retrieve glossary node " + parentUrn);
                }
                GlossaryNode node = GlossaryNodeMapper.map(context, response);
                if (canViewRelationship(
                    context.getOperationContext(), UrnUtils.getUrn(node.getUrn()), sourceUrn)) {
                  viewable.add(node);
                }
              }
            }

            final ParentNodesResult result = new ParentNodesResult();
            result.setCount(viewable.size());
            result.setNodes(viewable);
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to load parent nodes", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
