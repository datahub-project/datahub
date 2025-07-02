package com.linkedin.datahub.upgrade.config;

import java.util.Objects;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ReindexDebugCondition implements Condition {
  public static final String DEBUG_REINDEX = "ReindexDebug";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    ApplicationArguments bean = context.getBeanFactory().getBean(ApplicationArguments.class);
    return bean.getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(DEBUG_REINDEX::equalsIgnoreCase);
  }
}
