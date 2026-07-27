package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.getQueryContext;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GetRootEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetRootMetricsResult;
import com.linkedin.datahub.graphql.generated.Metric;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GetRootMetricsResolver
    implements DataFetcher<CompletableFuture<GetRootMetricsResult>> {

  static final String HAS_PARENT_METRIC_FIELD_NAME = "hasParentMetric";
  private static final String DEFAULT_QUERY = "*";

  private final EntityClient _entityClient;

  public GetRootMetricsResolver(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  @Override
  public CompletableFuture<GetRootMetricsResult> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = getQueryContext(environment);
    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final GetRootEntitiesInput input =
              bindArgument(environment.getArgument("input"), GetRootEntitiesInput.class);
          final Integer start = input.getStart() == null ? 0 : input.getStart();
          final Integer count = input.getCount() == null ? 25 : input.getCount();
          final String query =
              input.getQuery() == null || input.getQuery().isEmpty()
                  ? DEFAULT_QUERY
                  : input.getQuery();
          final Filter filter = buildRootMetricsFilter();

          try {
            final SearchResult gmsResult =
                _entityClient.search(
                    context.getOperationContext().withSearchFlags(flags -> flags.setFulltext(true)),
                    Constants.METRIC_ENTITY_NAME,
                    query,
                    filter,
                    Collections.singletonList(
                        new SortCriterion().setField("name").setOrder(SortOrder.ASCENDING)),
                    start,
                    count);

            final GetRootMetricsResult result = new GetRootMetricsResult();
            result.setStart(gmsResult.getFrom());
            result.setCount(gmsResult.getPageSize());
            result.setTotal(gmsResult.getNumEntities());
            result.setMetrics(
                mapUnresolvedMetrics(
                    gmsResult.getEntities().stream()
                        .map(SearchEntity::getEntity)
                        .collect(Collectors.toList())));
            return result;
          } catch (RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve root metrics from GMS", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private Filter buildRootMetricsFilter() {
    final CriterionArray array =
        new CriterionArray(
            ImmutableList.of(
                buildCriterion(HAS_PARENT_METRIC_FIELD_NAME, Condition.EQUAL, "false")));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }

  private List<Metric> mapUnresolvedMetrics(final List<Urn> entityUrns) {
    final List<Metric> results = new ArrayList<>();
    for (final Urn urn : entityUrns) {
      final Metric stub = new Metric();
      stub.setUrn(urn.toString());
      stub.setType(EntityType.METRIC);
      results.add(stub);
    }
    return results;
  }
}
