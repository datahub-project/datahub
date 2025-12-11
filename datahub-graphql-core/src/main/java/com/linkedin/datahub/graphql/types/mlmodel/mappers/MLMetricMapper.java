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
import com.linkedin.datahub.graphql.generated.MLMetric;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MLMetricMapper implements ModelMapper<com.linkedin.ml.metadata.MLMetric, MLMetric> {

  public static final MLMetricMapper INSTANCE = new MLMetricMapper();

  public static MLMetric map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.MLMetric metric) {
    return INSTANCE.apply(context, metric);
  }

  @Override
  public MLMetric apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.MLMetric metric) {
    final MLMetric result = new MLMetric();
    result.setDescription(metric.getDescription());
    result.setValue(metric.getValue());
    result.setCreatedAt(metric.getCreatedAt());
    result.setName(metric.getName());
    return result;
  }
}
