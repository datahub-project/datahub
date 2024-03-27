package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MLModelFactorPrompts;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLModelFactorPromptsMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLModelFactorPrompts, MLModelFactorPrompts> {

  public static final MLModelFactorPromptsMapper INSTANCE = new MLModelFactorPromptsMapper();

  public static MLModelFactorPrompts map(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelFactorPrompts input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public MLModelFactorPrompts apply(
      @Nullable QueryContext context,
      @NonNull final com.linkedin.ml.metadata.MLModelFactorPrompts input) {
    final MLModelFactorPrompts mlModelFactorPrompts = new MLModelFactorPrompts();
    if (input.getEvaluationFactors() != null) {
      mlModelFactorPrompts.setEvaluationFactors(
          input.getEvaluationFactors().stream()
              .map(f -> MLModelFactorsMapper.map(context, f))
              .collect(Collectors.toList()));
    }
    if (input.getRelevantFactors() != null) {
      mlModelFactorPrompts.setRelevantFactors(
          input.getRelevantFactors().stream()
              .map(f -> MLModelFactorsMapper.map(context, f))
              .collect(Collectors.toList()));
    }
    return mlModelFactorPrompts;
  }
}
