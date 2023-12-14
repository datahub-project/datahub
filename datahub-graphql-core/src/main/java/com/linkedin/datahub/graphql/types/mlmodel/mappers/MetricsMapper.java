package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.Metrics;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import lombok.NonNull;

public class MetricsMapper implements ModelMapper<com.linkedin.ml.metadata.Metrics, Metrics> {

  public static final MetricsMapper INSTANCE = new MetricsMapper();

  public static Metrics map(@NonNull final com.linkedin.ml.metadata.Metrics metrics) {
    return INSTANCE.apply(metrics);
  }

  @Override
  public Metrics apply(@NonNull final com.linkedin.ml.metadata.Metrics metrics) {
    final Metrics result = new Metrics();
    result.setDecisionThreshold(metrics.getDecisionThreshold());
    result.setPerformanceMeasures(metrics.getPerformanceMeasures());
    return result;
  }
}
