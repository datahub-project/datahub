package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.WindowDuration;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.OperationsQueryResult;
import com.linkedin.datahub.graphql.generated.OperationsStatsInput;
import com.linkedin.datahub.graphql.types.operations.OperationsQueryResultMapper;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.timeseries.elastic.OperationsServiceUtil;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.usage.UsageTimeRange;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatasetOperationsStatsResolver
    implements DataFetcher<CompletableFuture<OperationsQueryResult>> {

  private static final UsageTimeRange DEFAULT_RANGE = UsageTimeRange.MONTH;

  private final TimeseriesAspectService timeseriesAspectService;

  public DatasetOperationsStatsResolver(final TimeseriesAspectService timeseriesAspectService) {
    this.timeseriesAspectService = timeseriesAspectService;
  }

  @Override
  public CompletableFuture<OperationsQueryResult> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn resourceUrn = UrnUtils.getUrn(((Entity) environment.getSource()).getUrn());
    final OperationsStatsInput input =
        bindArgument(environment.getArgument("input"), OperationsStatsInput.class);
    final UsageTimeRange range =
        input.getRange() != null
            ? UsageTimeRange.valueOf(input.getRange().toString())
            : DEFAULT_RANGE;
    final String timeZone = input.getTimeZone();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            if (!AuthorizationUtils.isViewDatasetOperationsAuthorized(context, resourceUrn)) {
              log.debug(
                  "User {} is not authorized to view operation information for dataset {}",
                  context.getActorUrn(),
                  resourceUrn);
              // don't cause loading the whole dataset to fail
              return new OperationsQueryResult();
            }
            com.linkedin.operations.OperationsQueryResult result =
                OperationsServiceUtil.queryRange(
                    context.getOperationContext(),
                    timeseriesAspectService,
                    resourceUrn.toString(),
                    WindowDuration.DAY,
                    range,
                    timeZone);
            return OperationsQueryResultMapper.map(context, result);
          } catch (Exception e) {
            log.error(
                String.format("Failed to load Operations Stats for resource %s", resourceUrn), e);
            MetricUtils.counter(this.getClass(), "operations_stats_dropped").inc();
          }
          // don't cause loading the whole dataset to fail
          return new OperationsQueryResult();
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
