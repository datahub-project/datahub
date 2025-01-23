package com.linkedin.datahub.graphql.resolvers.container;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.ContainerEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/** Retrieves a list of historical executions for a particular source. */
@Slf4j
public class ContainerEntitiesResolver implements DataFetcher<CompletableFuture<SearchResults>> {

  static final List<String> CONTAINABLE_ENTITY_NAMES =
      ImmutableList.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME);
  private static final String CONTAINER_FIELD_NAME = "container";
  private static final String INPUT_ARG_NAME = "input";
  private static final String DEFAULT_QUERY = "*";
  private static final Integer DEFAULT_START = 0;
  private static final Integer DEFAULT_COUNT = 20;
  private static final ContainerEntitiesInput DEFAULT_ENTITIES_INPUT = new ContainerEntitiesInput();

  static {
    DEFAULT_ENTITIES_INPUT.setQuery(DEFAULT_QUERY);
    DEFAULT_ENTITIES_INPUT.setStart(DEFAULT_START);
    DEFAULT_ENTITIES_INPUT.setCount(DEFAULT_COUNT);
  }

  private final EntityClient _entityClient;

  public ContainerEntitiesResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<SearchResults> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final String urn = ((Container) environment.getSource()).getUrn();

    final ContainerEntitiesInput input =
        environment.getArgument(INPUT_ARG_NAME) != null
            ? bindArgument(environment.getArgument(INPUT_ARG_NAME), ContainerEntitiesInput.class)
            : DEFAULT_ENTITIES_INPUT;

    final String query = input.getQuery() != null ? input.getQuery() : "*";
    final int start = input.getStart() != null ? input.getStart() : 0;
    final int count = input.getCount() != null ? input.getCount() : 20;

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {

            final Criterion filterCriterion =
                buildCriterion(CONTAINER_FIELD_NAME + ".keyword", Condition.EQUAL, urn);

            return UrnSearchResultsMapper.map(
                context,
                _entityClient.searchAcrossEntities(
                    context.getOperationContext(),
                    CONTAINABLE_ENTITY_NAMES,
                    query,
                    new Filter()
                        .setOr(
                            new ConjunctiveCriterionArray(
                                new ConjunctiveCriterion()
                                    .setAnd(
                                        new CriterionArray(ImmutableList.of(filterCriterion))))),
                    start,
                    count,
                    Collections.emptyList(),
                    null));

          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to resolve entities associated with container with urn %s", urn),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
