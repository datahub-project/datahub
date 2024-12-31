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
