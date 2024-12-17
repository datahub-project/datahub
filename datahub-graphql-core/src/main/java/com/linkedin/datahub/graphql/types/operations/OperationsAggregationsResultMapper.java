package com.linkedin.datahub.graphql.types.operations;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.OperationsAggregationsResult;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OperationsAggregationsResultMapper
    implements ModelMapper<
        com.linkedin.operations.OperationsAggregationsResult, OperationsAggregationsResult> {

  public static final OperationsAggregationsResultMapper INSTANCE =
      new OperationsAggregationsResultMapper();

  public static OperationsAggregationsResult map(
      @Nullable final QueryContext context,
      @Nonnull
          final com.linkedin.operations.OperationsAggregationsResult
              pdlOperationsResultAggregations) {
    return INSTANCE.apply(context, pdlOperationsResultAggregations);
  }

  @Override
  public OperationsAggregationsResult apply(
      @Nullable final QueryContext context,
      @Nonnull
          final com.linkedin.operations.OperationsAggregationsResult
              pdlOperationsResultAggregations) {
    OperationsAggregationsResult result = new OperationsAggregationsResult();
    result.setTotalOperations(pdlOperationsResultAggregations.getTotalOperations());
    result.setTotalDeletes(pdlOperationsResultAggregations.getTotalDeletes());
    result.setTotalInserts(pdlOperationsResultAggregations.getTotalInserts());
    result.setTotalUpdates(pdlOperationsResultAggregations.getTotalUpdates());
    return result;
  }
}
