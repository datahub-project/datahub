package com.linkedin.gms.factory.search;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when both {@code elasticsearch.enabled} and {@code elasticsearch.entityIndex.v3.enabled}
 * are true (same as {@link
 * com.linkedin.metadata.config.search.ElasticSearchConfiguration#isEffectiveEntityIndexV3Enabled()}).
 */
public final class EntityIndexV3EnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Boolean esIntegration =
        context.getEnvironment().getProperty("elasticsearch.enabled", Boolean.class);
    Boolean rawV3 =
        context.getEnvironment().getProperty("elasticsearch.entityIndex.v3.enabled", Boolean.class);
    return Boolean.TRUE.equals(esIntegration) && Boolean.TRUE.equals(rawV3);
  }
}
