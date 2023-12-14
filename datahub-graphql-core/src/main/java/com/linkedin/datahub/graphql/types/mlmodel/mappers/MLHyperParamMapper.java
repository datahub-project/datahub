package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.MLHyperParam;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class MLHyperParamMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLHyperParam, MLHyperParam> {

  public static final MLHyperParamMapper INSTANCE = new MLHyperParamMapper();

  public static MLHyperParam map(@NonNull final com.linkedin.ml.metadata.MLHyperParam input) {
    return INSTANCE.apply(input);
  }

  @Override
  public MLHyperParam apply(@NonNull final com.linkedin.ml.metadata.MLHyperParam input) {
    final MLHyperParam result = new MLHyperParam();

    result.setDescription(input.getDescription());
    result.setValue(input.getValue());
    result.setCreatedAt(input.getCreatedAt());
    result.setName(input.getName());
    return result;
  }
}
