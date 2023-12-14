package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class UsageQueryResultMapper
    implements ModelMapper<com.linkedin.usage.UsageQueryResult, UsageQueryResult> {

  public static final UsageQueryResultMapper INSTANCE = new UsageQueryResultMapper();

  public static UsageQueryResult map(
      @Nonnull final com.linkedin.usage.UsageQueryResult pdlUsageResult) {
    return INSTANCE.apply(pdlUsageResult);
  }

  @Override
  public UsageQueryResult apply(@Nonnull final com.linkedin.usage.UsageQueryResult pdlUsageResult) {
    UsageQueryResult result = new UsageQueryResult();
    if (pdlUsageResult.hasAggregations()) {
      result.setAggregations(
          UsageQueryResultAggregationMapper.map(pdlUsageResult.getAggregations()));
    }
    if (pdlUsageResult.hasBuckets()) {
      result.setBuckets(
          pdlUsageResult.getBuckets().stream()
              .map(bucket -> UsageAggregationMapper.map(bucket))
              .collect(Collectors.toList()));
    }
    return result;
  }
}
