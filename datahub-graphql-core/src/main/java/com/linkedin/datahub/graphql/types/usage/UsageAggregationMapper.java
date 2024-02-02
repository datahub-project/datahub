package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.generated.UsageAggregation;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;

public class UsageAggregationMapper
    implements ModelMapper<com.linkedin.usage.UsageAggregation, UsageAggregation> {

  public static final UsageAggregationMapper INSTANCE = new UsageAggregationMapper();

  public static UsageAggregation map(
      @Nonnull final com.linkedin.usage.UsageAggregation pdlUsageAggregation) {
    return INSTANCE.apply(pdlUsageAggregation);
  }

  @Override
  public UsageAggregation apply(
      @Nonnull final com.linkedin.usage.UsageAggregation pdlUsageAggregation) {
    UsageAggregation result = new UsageAggregation();
    result.setBucket(pdlUsageAggregation.getBucket());

    if (pdlUsageAggregation.hasDuration()) {
      result.setDuration(WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString()));
    }
    if (pdlUsageAggregation.hasResource()) {
      result.setResource(pdlUsageAggregation.getResource().toString());
    }
    if (pdlUsageAggregation.hasMetrics()) {
      result.setMetrics(UsageAggregationMetricsMapper.map(pdlUsageAggregation.getMetrics()));
    }
    return result;
  }
}
