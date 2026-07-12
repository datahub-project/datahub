package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

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

            List<GlossaryNode> viewable =
                parentUrns.stream()
                    .map(parentUrn -> loadGlossaryNode(context, parentUrn))
                    .filter(
                        node ->
                            canViewRelationship(
                                context.getOperationContext(),
                                UrnUtils.getUrn(node.getUrn()),
                                sourceUrn))
                    .collect(Collectors.toList());

            final ParentNodesResult result = new ParentNodesResult();
            result.setCount(viewable.size());
            result.setNodes(viewable);
            return result;
          } catch (DataHubGraphQLException e) {
            throw new RuntimeException("Failed to load parent nodes", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private GlossaryNode loadGlossaryNode(@Nonnull QueryContext context, @Nonnull Urn parentUrn) {
    try {
      EntityResponse response =
          _entityClient.getV2(
              context.getOperationContext(), parentUrn.getEntityType(), parentUrn, null);
      if (response == null) {
        throw new RuntimeException("Failed to retrieve glossary node " + parentUrn);
      }
      return GlossaryNodeMapper.map(context, response);
    } catch (Exception e) {
      throw new RuntimeException("Failed to retrieve glossary node " + parentUrn, e);
    }
  }
}
