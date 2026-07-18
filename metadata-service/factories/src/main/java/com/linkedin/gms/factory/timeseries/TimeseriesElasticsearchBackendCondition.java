package com.linkedin.gms.factory.timeseries;

import javax.annotation.Nullable;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Registers {@link com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService}
 * when {@code elasticsearch.enabled=true} (default from {@code application.yaml}) and {@code
 * timeseriesAspectService.implementation} is the search-cluster backend. {@code elasticsearch} and
 * {@code opensearch} are equivalent here: both use the same factory and OpenSearch/Elasticsearch
 * client shim. {@code postgres} uses only {@link
 * com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService}. Any other value is
 * treated as disabled for this bean (invalid config should be caught at startup when the primary
 * {@code timeseriesAspectService} is resolved).
 */
public final class TimeseriesElasticsearchBackendCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    if (!Boolean.TRUE.equals(
        context.getEnvironment().getProperty("elasticsearch.enabled", Boolean.class))) {
      return false;
    }
    String v =
        context
            .getEnvironment()
            .getProperty("timeseriesAspectService.implementation", "elasticsearch");
    return useElasticSearchTimeseriesService(v);
  }

  /**
   * {@code elasticsearch} and {@code opensearch} share one implementation; {@code postgres} does
   * not use this bean.
   */
  static boolean useElasticSearchTimeseriesService(@Nullable String raw) {
    if (raw == null) {
      return true;
    }
    String v = raw.trim();
    if (v.isEmpty()) {
      return true;
    }
    if ("postgres".equalsIgnoreCase(v)) {
      return false;
    }
    return "elasticsearch".equalsIgnoreCase(v) || "opensearch".equalsIgnoreCase(v);
  }
}
