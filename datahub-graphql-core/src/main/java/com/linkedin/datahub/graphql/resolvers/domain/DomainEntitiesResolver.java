package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Resolves the entities in a particular Domain. */
@Slf4j
public class DomainEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  private static final String DOMAINS_FIELD_NAME = "domains";
  private static final String INPUT_ARG_NAME = "input";
  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final DomainEntitiesInput DEFAULT_ENTITIES_INPUT = new DomainEntitiesInput();

  static {
    DEFAULT_ENTITIES_INPUT.setQuery(DEFAULT_QUERY);
    DEFAULT_ENTITIES_INPUT.setStart(DEFAULT_START);
    DEFAULT_ENTITIES_INPUT.setCount(DEFAULT_COUNT);
  }

  private final EntityClient _entityClient;

  public DomainEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((Domain) environment.getSource()).getUrn();

    final DomainEntitiesInput input =
        environment.getArgument(INPUT_ARG_NAME) != null
            ? bindArgument(environment.getArgument(INPUT_ARG_NAME), DomainEntitiesInput.class)
            : DEFAULT_ENTITIES_INPUT;

    final String query = input.getQuery() != null ? input.getQuery() : DEFAULT_QUERY;
    final int start = input.getStart() != null ? input.getStart() : DEFAULT_START;
    final int count = input.getCount() != null ? input.getCount() : DEFAULT_COUNT;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            final CriterionArray criteria = new CriterionArray();
            final Criterion filterCriterion =
                buildCriterion(DOMAINS_FIELD_NAME + ".keyword", Condition.EQUAL, urn);
            criteria.add(filterCriterion);
            if (input.getFilters() != null) {
              input
                  .getFilters()
                  .forEach(
                      filter -> {
                        criteria.add(criterionFromFilter(filter));
                      });
            }

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    SEARCHABLE_ENTITY_TYPES.stream()
                        .map(EntityTypeMapper::getName)
                        .collect(Collectors.toList()),
                    query,
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion().setAnd(criteria))),
                    start,
                    count,
                    Collections.emptyList()));

          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to resolve entities associated with Domain with urn %s", urn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
