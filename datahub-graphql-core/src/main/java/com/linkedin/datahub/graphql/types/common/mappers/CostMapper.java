package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Cost;
import com.linkedin.datahub.graphql.generated.CostType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import lombok.NonNull;

public class CostMapper implements ModelMapper<com.linkedin.common.Cost, Cost> {

  public static final CostMapper INSTANCE = new CostMapper();

  public static Cost map(@NonNull final com.linkedin.common.Cost cost) {
    return INSTANCE.apply(cost);
  }

  @Override
  public Cost apply(@Nonnull final com.linkedin.common.Cost cost) {
    final Cost result = new Cost();
    result.setCostType(CostType.valueOf(cost.getCostType().name()));
    result.setCostValue(CostValueMapper.map(cost.getCost()));
    return result;
  }
}
