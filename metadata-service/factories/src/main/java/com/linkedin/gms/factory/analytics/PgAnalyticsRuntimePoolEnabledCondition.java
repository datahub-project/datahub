package com.linkedin.gms.factory.analytics;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Enables the dedicated pgAnalytics Ebean pool only when Postgres is the active usage SoT. */
public class PgAnalyticsRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    boolean enabled =
        Boolean.TRUE.equals(
            context.getEnvironment().getProperty("postgres.pgAnalytics.enabled", Boolean.class));
    String implementation =
        context
            .getEnvironment()
            .getProperty("platformAnalytics.usage-events.implementation", "elasticsearch");
    if (implementation == null || implementation.isBlank()) {
      implementation =
          context.getEnvironment().getProperty("DATAHUB_USAGE_EVENTS_IMPLEMENTATION", "");
    }
    return enabled && "postgres".equalsIgnoreCase(implementation.trim());
  }
}
