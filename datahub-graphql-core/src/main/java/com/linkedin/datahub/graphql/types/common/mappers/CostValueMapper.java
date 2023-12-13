package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.CostValue;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class CostValueMapper implements ModelMapper<com.linkedin.common.CostValue, CostValue> {
  public static final CostValueMapper INSTANCE = new CostValueMapper();

  public static CostValue map(@NonNull final com.linkedin.common.CostValue costValue) {
    return INSTANCE.apply(costValue);
  }

  @Override
  public CostValue apply(@NonNull final com.linkedin.common.CostValue costValue) {
    final CostValue result = new CostValue();
    if (costValue.isCostCode()) {
      result.setCostCode(costValue.getCostCode());
    }
    if (costValue.isCostId()) {
      result.setCostId(costValue.getCostId().floatValue());
    }
    return result;
  }
}
