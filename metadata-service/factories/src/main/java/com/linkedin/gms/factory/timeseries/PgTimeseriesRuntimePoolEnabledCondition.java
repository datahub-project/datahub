package com.linkedin.gms.factory.timeseries;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Enables the dedicated pgTimeseries Ebean pool only when Postgres is the active backend. */
public class PgTimeseriesRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    boolean enabled =
        Boolean.TRUE.equals(
            context.getEnvironment().getProperty("postgres.pgTimeseries.enabled", Boolean.class));
    String implementation =
        context
            .getEnvironment()
            .getProperty("timeseriesAspectService.implementation", "elasticsearch");
    return enabled
        && TimeseriesPostgresBackendCondition.usePostgresTimeseriesService(implementation);
  }
}
