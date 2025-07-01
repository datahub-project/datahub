package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.ListDomainsInput;
import com.linkedin.datahub.graphql.generated.ListDomainsResult;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Resolver used for listing all Domains defined within DataHub. Requires the MANAGE_DOMAINS
 * platform privilege.
 */
public class ListDomainsResolver implements DataFetcher<CompletableFuture<ListDomainsResult>> {
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final String DEFAULT_QUERY = "";

  private final EntityClient _entityClient;

  public ListDomainsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<ListDomainsResult> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final ListDomainsInput input =
              bindArgument(environment.getArgument("input"), ListDomainsInput.class);
          final Integer start = input.getStart() == null ? DEFAULT_START : input.getStart();
          final Integer count = input.getCount() == null ? DEFAULT_COUNT : input.getCount();
          final String query = input.getQuery() == null ? DEFAULT_QUERY : input.getQuery();
          final Urn parentDomainUrn =
              input.getParentDomain() != null ? UrnUtils.getUrn(input.getParentDomain()) : null;
          final Filter filter = DomainUtils.buildParentDomainFilter(parentDomainUrn);

          try {
            // First, get all domain Urns.
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.DOMAIN_ENTITY_NAME,
                    query,
                    filter,
                    Collections.singletonList(
                        new SortCriterion()
                            .setField(DOMAIN_CREATED_TIME_INDEX_FIELD_NAME)
                            .setOrder(SortOrder.DESCENDING)),
                    start,
                    count);

            // Now that we have entities we can bind this to a result.
            final ListDomainsResult result = new ListDomainsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setDomains(
                mapUnresolvedDomains(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (Exception e) {
            throw new RuntimeException("Failed to list domains", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  // This method maps urns returned from the list endpoint into Partial Domain objects which will be
  // resolved be a separate Batch resolver.
  private List<Domain> mapUnresolvedDomains(final List<Urn> entityUrns) {
    final List<Domain> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final Domain unresolvedDomain = new Domain();
      unresolvedDomain.setUrn(urn.toString());
      unresolvedDomain.setType(EntityType.DOMAIN);
      results.add(unresolvedDomain);
    }
    return results;
  }
}
