package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CostValue;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class CostValueMapper implements ModelMapper<com.linkedin.common.CostValue, CostValue> {
  public static final CostValueMapper INSTANCE = new CostValueMapper();

  public static CostValue map(
      @Nullable QueryContext context, @NonNull final com.linkedin.common.CostValue costValue) {
    return INSTANCE.apply(context, costValue);
  }

  @Override
  public CostValue apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.common.CostValue costValue) {
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
