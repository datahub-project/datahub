package com.linkedin.datahub.upgrade.conditions;

import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class GeneralUpgradeCondition implements Condition {
  public static final String LOAD_INDICES_ARG = "LoadIndices";
  public static final Set<String> EXCLUDED_ARGS = Set.of(LOAD_INDICES_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // This condition matches when LoadIndices is NOT in the arguments
    return !context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(EXCLUDED_ARGS::contains);
  }
}
