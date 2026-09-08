package com.linkedin.gms.factory.graph;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Dedicated pgGraph Ebean pool only when Postgres is the exclusive graph SoT. */
public class PgGraphRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    boolean enabled =
        Boolean.TRUE.equals(
            context.getEnvironment().getProperty("postgres.pgGraph.enabled", Boolean.class));
    String type = context.getEnvironment().getProperty("graphService.type", "elasticsearch");
    return enabled && GraphPostgresBackendCondition.usePostgresGraphService(type);
  }
}
