package com.linkedin.gms.factory.systemmetadata;

import javax.annotation.Nullable;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Registers PostgreSQL system-metadata beans when {@code systemMetadataService.implementation} is
 * {@code postgres}.
 */
public final class SystemMetadataPostgresBackendCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String v =
        context
            .getEnvironment()
            .getProperty("systemMetadataService.implementation", "elasticsearch");
    return usePostgresSystemMetadataService(v);
  }

  static boolean usePostgresSystemMetadataService(@Nullable String raw) {
    if (raw == null) {
      return false;
    }
    return "postgres".equalsIgnoreCase(raw.trim());
  }
}
