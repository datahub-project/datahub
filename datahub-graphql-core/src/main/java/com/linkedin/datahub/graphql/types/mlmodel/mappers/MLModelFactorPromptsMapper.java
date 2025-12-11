/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
