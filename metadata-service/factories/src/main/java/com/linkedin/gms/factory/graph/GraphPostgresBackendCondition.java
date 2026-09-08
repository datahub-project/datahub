package com.linkedin.gms.factory.graph;

import javax.annotation.Nullable;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Registers PostgreSQL graph beans when {@code graphService.type} is {@code postgres}. */
public final class GraphPostgresBackendCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String v = context.getEnvironment().getProperty("graphService.type", "elasticsearch");
    return usePostgresGraphService(v);
  }

  static boolean usePostgresGraphService(@Nullable String raw) {
    if (raw == null) {
      return false;
    }
    return "postgres".equalsIgnoreCase(raw.trim());
  }
}
