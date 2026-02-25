package com.linkedin.datahub.upgrade.conditions;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class LoadIndicesCondition implements Condition {
  public static final String LOAD_INDICES_ARG = "LoadIndices";
  public static final Set<String> LOAD_INDICES_ARGS = Set.of(LOAD_INDICES_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return false;
    }
    return nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(LOAD_INDICES_ARGS::contains);
  }
}
