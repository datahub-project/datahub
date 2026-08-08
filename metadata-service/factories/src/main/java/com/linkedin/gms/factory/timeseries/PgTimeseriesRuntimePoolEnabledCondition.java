package com.linkedin.gms.factory.timeseries;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Enables the dedicated pgTimeseries Ebean pool when SqlSetup pgTimeseries is enabled. */
public class PgTimeseriesRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return Boolean.TRUE.equals(
        context.getEnvironment().getProperty("postgres.pgTimeseries.enabled", Boolean.class));
  }
}
