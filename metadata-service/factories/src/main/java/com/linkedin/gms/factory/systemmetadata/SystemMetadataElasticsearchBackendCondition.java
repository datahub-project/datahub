package com.linkedin.gms.factory.systemmetadata;

import javax.annotation.Nullable;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** Elasticsearch system-metadata scroll client when SoT is not postgres. */
public final class SystemMetadataElasticsearchBackendCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String v =
        context
            .getEnvironment()
            .getProperty("systemMetadataService.implementation", "elasticsearch");
    return !usePostgres(v);
  }

  static boolean usePostgres(@Nullable String raw) {
    return raw != null && "postgres".equalsIgnoreCase(raw.trim());
  }
}
