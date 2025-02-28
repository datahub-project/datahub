package com.linkedin.datahub.graphql.resolvers.dashboard;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;
import static com.linkedin.metadata.utils.CriterionUtils.buildIsNullCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.DashboardUsageAggregation;
import com.linkedin.datahub.graphql.generated.DashboardUsageMetrics;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResult;
import com.linkedin.datahub.graphql.generated.DashboardUsageQueryResultAggregations;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.dashboard.mappers.DashboardUsageMetricMapper;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver used for resolving the usage statistics of a Dashboard.
 *
 * <p>Returns daily as well as absolute usage metrics of Dashboard
 */
@Slf4j
public class DashboardUsageStatsResolver
    implements DataFetcher<CompletableFuture<DashboardUsageQueryResult>> {
  private static final String ES_FIELD_EVENT_GRANULARITY = "eventGranularity";
  private final TimeseriesAspectService timeseriesAspectService;

  public DashboardUsageStatsResolver(TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
  }

  @Override
  public CompletableFuture<DashboardUsageQueryResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final String dashboardUrn = ((Entity) environment.getSource()).getUrn();
    final Long maybeStartTimeMillis = environment.getArgumentOrDefault("startTimeMillis", null);
    final Long maybeEndTimeMillis = environment.getArgumentOrDefault("endTimeMillis", null);
    // Max number of aspects to return for absolute dashboard usage.
    final Integer maybeLimit = environment.getArgumentOrDefault("limit", null);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          DashboardUsageQueryResult usageQueryResult = new DashboardUsageQueryResult();

          // Time Bucket Stats
          Filter bucketStatsFilter =
              createUsageFilter(dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, true);
          List<DashboardUsageAggregation> dailyUsageBuckets =
              getBuckets(
                  context.getOperationContext(),
                  bucketStatsFilter,
                  dashboardUrn,
                  timeseriesAspectService);
          DashboardUsageQueryResultAggregations aggregations =
              getAggregations(
                  context.getOperationContext(),
                  bucketStatsFilter,
                  dailyUsageBuckets,
                  timeseriesAspectService);

          usageQueryResult.setBuckets(dailyUsageBuckets);
          usageQueryResult.setAggregations(aggregations);

          // Absolute usage metrics
          List<DashboardUsageMetrics> dashboardUsageMetrics =
              getDashboardUsageMetrics(
                  context, dashboardUrn, maybeStartTimeMillis, maybeEndTimeMillis, maybeLimit);
          usageQueryResult.setMetrics(dashboardUsageMetrics);
          return usageQueryResult;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private List<DashboardUsageMetrics> getDashboardUsageMetrics(
      @Nullable QueryContext context,
      String dashboardUrn,
      Long maybeStartTimeMillis,
      Long maybeEndTimeMillis,
      Integer maybeLimit) {
    List<DashboardUsageMetrics> dashboardUsageMetrics;
    try {
      Filter filter = new Filter();
      final ArrayList<Criterion> criteria = new ArrayList<>();

      // Add filter for absence of eventGranularity - only consider absolute stats
      Criterion excludeTimeBucketsCriterion = buildIsNullCriterion(ES_FIELD_EVENT_GRANULARITY);
      criteria.add(excludeTimeBucketsCriterion);
      filter.setOr(
          new ConjunctiveCriterionArray(
              ImmutableList.of(new ConjunctiveCriterion().setAnd(new CriterionArray(criteria)))));

      List<EnvelopedAspect> aspects =
          timeseriesAspectService.getAspectValues(
              context.getOperationContext(),
              Urn.createFromString(dashboardUrn),
              Constants.DASHBOARD_ENTITY_NAME,
              Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME,
              maybeStartTimeMillis,
              maybeEndTimeMillis,
              maybeLimit,
              filter);
      dashboardUsageMetrics =
          aspects.stream()
              .map(a -> DashboardUsageMetricMapper.map(context, a))
              .collect(Collectors.toList());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid resource", e);
    }
    return dashboardUsageMetrics;
  }
}
