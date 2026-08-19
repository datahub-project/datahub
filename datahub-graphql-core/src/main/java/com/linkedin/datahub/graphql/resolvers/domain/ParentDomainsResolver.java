package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentDomainsResult;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.metadata.graph.cache.client.BoundHierarchyAccess;
import com.linkedin.metadata.graph.cache.client.HierarchyBindings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ParentDomainsResolver implements DataFetcher<CompletableFuture<ParentDomainsResult>> {

  @Override
  public CompletableFuture<ParentDomainsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = getQueryContext(environment);
    final Urn urn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());

    if (!DOMAIN_ENTITY_NAME.equals(urn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Failed to resolve parents for entity type %s", urn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            List<Urn> parentUrns =
                BoundHierarchyAccess.orderedParents(
                    context.getOperationContext(),
                    HierarchyBindings.domainSpec(context.getOperationContext()),
                    urn,
                    context.getMaxParentDepth());

            List<Entity> viewable =
                parentUrns.stream()
                    .map(parentUrn -> UrnToEntityMapper.map(context, parentUrn))
                    .filter(
                        e ->
                            canViewRelationship(
                                context.getOperationContext(), UrnUtils.getUrn(e.getUrn()), urn))
                    .collect(Collectors.toList());

            final ParentDomainsResult result = new ParentDomainsResult();
            result.setCount(viewable.size());
            result.setDomains(viewable);
            return result;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to load parent domains for entity %s", urn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
