package com.linkedin.gms.factory.systemmetadata;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Dedicated pgSystemMetadata Ebean pool only when Postgres is the exclusive system-metadata SoT.
 */
public class PgSystemMetadataRuntimePoolEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    boolean enabled =
        Boolean.TRUE.equals(
            context
                .getEnvironment()
                .getProperty("postgres.pgSystemMetadata.enabled", Boolean.class));
    String implementation =
        context
            .getEnvironment()
            .getProperty("systemMetadataService.implementation", "elasticsearch");
    if (implementation == null || implementation.isBlank()) {
      implementation =
          context.getEnvironment().getProperty("SYSTEM_METADATA_SERVICE_IMPLEMENTATION", "");
    }
    return enabled && "postgres".equalsIgnoreCase(implementation.trim());
  }
}
