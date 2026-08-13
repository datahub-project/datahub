package com.linkedin.gms.factory.timeseries;

import javax.annotation.Nullable;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Registers {@link com.linkedin.metadata.timeseries.postgres.PostgresTimeseriesAspectService} when
 * {@code timeseriesAspectService.implementation} is {@code postgres} (case-insensitive, trimmed).
 */
public final class TimeseriesPostgresBackendCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String v =
        context
            .getEnvironment()
            .getProperty("timeseriesAspectService.implementation", "elasticsearch");
    return usePostgresTimeseriesService(v);
  }

  static boolean usePostgresTimeseriesService(@Nullable String raw) {
    if (raw == null) {
      return false;
    }
    return "postgres".equalsIgnoreCase(raw.trim());
  }
}
