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
import com.linkedin.datahub.graphql.generated.Metrics;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nullable;
import lombok.NonNull;

public class MetricsMapper implements ModelMapper<com.linkedin.ml.metadata.Metrics, Metrics> {

  public static final MetricsMapper INSTANCE = new MetricsMapper();

  public static Metrics map(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.Metrics metrics) {
    return INSTANCE.apply(context, metrics);
  }

  @Override
  public Metrics apply(
      @Nullable QueryContext context, @NonNull final com.linkedin.ml.metadata.Metrics metrics) {
    final Metrics result = new Metrics();
    result.setDecisionThreshold(metrics.getDecisionThreshold());
    result.setPerformanceMeasures(metrics.getPerformanceMeasures());
    return result;
  }
}
