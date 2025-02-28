package com.linkedin.datahub.upgrade.config;

import java.util.Objects;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class EvaluateTestsCondition implements Condition {
  public static final String TESTS_ARG = "EvaluateTests";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(TESTS_ARG::contains);
  }
}
