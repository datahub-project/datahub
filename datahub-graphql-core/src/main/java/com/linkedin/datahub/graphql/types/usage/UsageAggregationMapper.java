package com.linkedin.datahub.graphql.types.usage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UsageAggregation;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class UsageAggregationMapper
    implements ModelMapper<com.linkedin.usage.UsageAggregation, UsageAggregation> {

  public static final UsageAggregationMapper INSTANCE = new UsageAggregationMapper();

  public static UsageAggregation map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.usage.UsageAggregation pdlUsageAggregation) {
    return INSTANCE.apply(context, pdlUsageAggregation);
  }

  @Override
  public UsageAggregation apply(
      @Nullable QueryContext context,
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
      result.setMetrics(
          UsageAggregationMetricsMapper.map(context, pdlUsageAggregation.getMetrics()));
    }
    return result;
  }
}
