package com.linkedin.datahub.graphql.resolvers.domain;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


/**
 * Resolves the entities in a particular Domain.
 */
@Slf4j
public class DomainEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String DOMAINS_FIELD_NAME = "domains";

  private final EntityClient _entityClient;

  public DomainEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((Domain) environment.getSource()).getUrn();

    final DomainEntitiesInput input = bindArgument(environment.getArgument("input"), DomainEntitiesInput.class);

    final String query = input.getQuery() != null ? input.getQuery() : "*";
    final int start = input.getStart() != null ? input.getStart() : 0;
    final int count = input.getCount() != null ? input.getCount() : 20;

    return CompletableFuture.supplyAsync(() -> {

      try {

        // 1. Fetch the related edges
        final Criterion filterCriterion =  new Criterion()
            .setField(DOMAINS_FIELD_NAME + ".keyword")
            .setCondition(Condition.EQUAL)
            .setValue(urn);

        return UrnSearchResultsMapper.map(_entityClient.searchAcrossEntities(
            Collections.emptyList(),
            query,
            new Filter().setOr(new ConjunctiveCriterionArray(
                new ConjunctiveCriterion().setAnd(new CriterionArray(ImmutableList.of(filterCriterion)))
            )),
            start,
            count,
            context.getAuthentication()
        ));

      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to resolve entities associated with Domain with urn %s", urn), e);
      }
    });
  }
}