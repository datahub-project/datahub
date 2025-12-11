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
import com.linkedin.datahub.graphql.generated.MLHyperParam;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLHyperParamMapper
    implements ModelMapper<com.linkedin.ml.metadata.MLHyperParam, MLHyperParam> {

  public static final MLHyperParamMapper INSTANCE = new MLHyperParamMapper();

  public static MLHyperParam map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.MLHyperParam input) {
    return INSTANCE.apply(context, input);
  }

  @Override
  public MLHyperParam apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.MLHyperParam input) {
    final MLHyperParam result = new MLHyperParam();

    result.setDescription(input.getDescription());
    result.setValue(input.getValue());
    result.setCreatedAt(input.getCreatedAt());
    result.setName(input.getName());
    return result;
  }
}
