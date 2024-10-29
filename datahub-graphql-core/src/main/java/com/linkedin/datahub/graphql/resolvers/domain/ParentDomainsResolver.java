package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canViewRelationship;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ParentDomainsResult;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ParentDomainsResolver implements DataFetcher<CompletableFuture<ParentDomainsResult>> {

  private final EntityClient _entityClient;

  public ParentDomainsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ParentDomainsResult> get(DataFetchingEnvironment environment) {
    final QueryContext context = environment.getContext();
    final Urn urn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final List<Entity> parentDomains = new ArrayList<>();
    final Set<String> visitedParentUrns = new HashSet<>();

    if (!DOMAIN_ENTITY_NAME.equals(urn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format("Failed to resolve parents for entity type %s", urn));
    }

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            Entity parentDomain = DomainUtils.getParentDomain(urn, context, _entityClient);

            while (parentDomain != null && !visitedParentUrns.contains(parentDomain.getUrn())) {
              parentDomains.add(parentDomain);
              visitedParentUrns.add(parentDomain.getUrn());
              parentDomain =
                  DomainUtils.getParentDomain(
                      Urn.createFromString(parentDomain.getUrn()), context, _entityClient);
            }

            List<Entity> viewable =
                parentDomains.stream()
                    .filter(
                        e ->
                            context == null
                                || canViewRelationship(
                                    context.getOperationContext(),
                                    UrnUtils.getUrn(e.getUrn()),
                                    urn))
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
