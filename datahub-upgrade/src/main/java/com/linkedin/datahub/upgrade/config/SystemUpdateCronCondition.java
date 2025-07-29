package com.linkedin.datahub.upgrade.config;

import java.util.Objects;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SystemUpdateCronCondition implements Condition {
  public static final String SYSTEM_UPDATE_CRON_ARG = "SystemUpdateCron";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(SYSTEM_UPDATE_CRON_ARG::equalsIgnoreCase);
  }
}
