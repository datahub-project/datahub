package com.linkedin.datahub.graphql.types.operations;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OperationsAggregation;
import com.linkedin.datahub.graphql.generated.WindowDuration;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OperationsAggregationMapper
    implements ModelMapper<com.linkedin.operations.OperationsAggregation, OperationsAggregation> {

  public static final OperationsAggregationMapper INSTANCE = new OperationsAggregationMapper();

  public static OperationsAggregation map(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsAggregation pdlUsageAggregation) {
    return INSTANCE.apply(context, pdlUsageAggregation);
  }

  @Override
  public OperationsAggregation apply(
      @Nullable QueryContext context,
      @Nonnull final com.linkedin.operations.OperationsAggregation pdlUsageAggregation) {
    OperationsAggregation result = new OperationsAggregation();
    result.setBucket(pdlUsageAggregation.getBucket());

    if (pdlUsageAggregation.hasDuration()) {
      result.setDuration(WindowDuration.valueOf(pdlUsageAggregation.getDuration().toString()));
    }
    if (pdlUsageAggregation.hasResource()) {
      result.setResource(pdlUsageAggregation.getResource().toString());
    }
    if (pdlUsageAggregation.hasAggregations()) {
      result.setAggregations(
          OperationsAggregationsResultMapper.map(context, pdlUsageAggregation.getAggregations()));
    }
    return result;
  }
}
