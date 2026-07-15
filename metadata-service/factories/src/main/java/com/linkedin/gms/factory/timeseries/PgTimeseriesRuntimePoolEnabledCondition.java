package com.linkedin.gms.factory.timeseries;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Enables the dedicated pgTimeseries Ebean pool when SqlSetup pgTimeseries is on, or when platform
 * analytics usage events use the PostgreSQL implementation (same pool as runtime timeseries
 * workloads).
 */
public class PgTimeseriesRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    if (Boolean.TRUE.equals(
        context.getEnvironment().getProperty("postgres.pgTimeseries.enabled", Boolean.class))) {
      return true;
    }
    String usageImpl =
        context
            .getEnvironment()
            .getProperty("platformAnalytics.usage-events.implementation", "elasticsearch");
    return "postgres".equalsIgnoreCase(usageImpl);
  }
}
