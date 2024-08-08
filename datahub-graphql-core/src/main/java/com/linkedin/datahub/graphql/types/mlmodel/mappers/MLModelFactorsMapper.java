package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLModelFactors;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.ArrayList;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLModelFactorsMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLModelFactors, MLModelFactors> {

  public static final MLModelFactorsMapper INSTANCE = new MLModelFactorsMapper();

  public static MLModelFactors map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelFactors modelFactors) {
    return INSTANCE.apply(context, modelFactors);
  }

  @Override
  public MLModelFactors apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelFactors mlModelFactors) {
    final MLModelFactors result = new MLModelFactors();
    if (mlModelFactors.getEnvironment() != null) {
      result.setEnvironment(new ArrayList<>(mlModelFactors.getEnvironment()));
    }
    if (mlModelFactors.getGroups() != null) {
      result.setGroups(new ArrayList<>(mlModelFactors.getGroups()));
    }
    if (mlModelFactors.getInstrumentation() != null) {
      result.setInstrumentation(new ArrayList<>(mlModelFactors.getInstrumentation()));
    }
    return result;
  }
}
