package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.generated.UsageQueryResultAggregations;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UsageQueryResultMapper
    implements ModelMapper<com.linkedin.usage.UsageQueryResult, UsageQueryResult> {

  public static final UsageQueryResult EMPTY =
      new UsageQueryResult(List.of(), new UsageQueryResultAggregations(0, List.of(), List.of(), 0));

  public static final UsageQueryResultMapper INSTANCE = new UsageQueryResultMapper();

  public static UsageQueryResult map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.usage.UsageQueryResult pdlUsageResult) {
    return INSTANCE.apply(context, pdlUsageResult);
  }

  @Override
  public UsageQueryResult apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UsageQueryResult pdlUsageResult) {
    UsageQueryResult result = new UsageQueryResult();
    if (pdlUsageResult.hasAggregations()) {
      result.setAggregations(
          UsageQueryResultAggregationMapper.map(context, pdlUsageResult.getAggregations()));
    }
    if (pdlUsageResult.hasBuckets()) {
      result.setBuckets(
          pdlUsageResult.getBuckets().stream()
              .map(bucket -> UsageAggregationMapper.map(context, bucket))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
