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
