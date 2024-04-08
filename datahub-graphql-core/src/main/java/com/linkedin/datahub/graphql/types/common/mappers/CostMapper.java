package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Cost;
import com.linkedin.datahub.graphql.generated.CostType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.NonNull;

public class CostMapper implements ModelMapper<com.linkedin.common.Cost, Cost> {

  public static final CostMapper INSTANCE = new CostMapper();

  public static Cost map(
      @Nullable QueryContext context, @NonNull final com.linkedin.common.Cost cost) {
    return INSTANCE.apply(context, cost);
  }

  @Override
  public Cost apply(@Nullable QueryContext context, @Nonnull final com.linkedin.common.Cost cost) {
    final Cost result = new Cost();
    result.setCostType(CostType.valueOf(cost.getCostType().name()));
    result.setCostValue(CostValueMapper.map(context, cost.getCost()));
    return result;
  }
}
