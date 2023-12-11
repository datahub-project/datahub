package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.MLModelFactorPrompts;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import lombok.NonNull;

public class MLModelFactorPromptsMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLModelFactorPrompts, MLModelFactorPrompts> {

  public static final MLModelFactorPromptsMapper INSTANCE = new MLModelFactorPromptsMapper();

  public static MLModelFactorPrompts map(
      @NonNull final com.linkedin.ml.metadata.MLModelFactorPrompts input) {
    return INSTANCE.apply(input);
  }

  @Override
  public MLModelFactorPrompts apply(
      @NonNull final com.linkedin.ml.metadata.MLModelFactorPrompts input) {
    final MLModelFactorPrompts mlModelFactorPrompts = new MLModelFactorPrompts();
    if (input.getEvaluationFactors() != null) {
      mlModelFactorPrompts.setEvaluationFactors(
          input.getEvaluationFactors().stream()
              .map(MLModelFactorsMapper::map)
              .collect(Collectors.toList()));
    }
    if (input.getRelevantFactors() != null) {
      mlModelFactorPrompts.setRelevantFactors(
          input.getRelevantFactors().stream()
              .map(MLModelFactorsMapper::map)
              .collect(Collectors.toList()));
    }
    return mlModelFactorPrompts;
  }
}
