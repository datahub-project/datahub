package com.linkedin.datahub.upgrade.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SystemUpdateCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .anyMatch("SystemUpdate"::equals);
  }
}
