package com.linkedin.datahub.upgrade.config;

import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SystemUpdateCondition implements Condition {
  public static final String SYSTEM_UPDATE_ARG = "SystemUpdate";
  public static final String BLOCKING_SYSTEM_UPDATE_ARG = SYSTEM_UPDATE_ARG + "Blocking";
  public static final String NONBLOCKING_SYSTEM_UPDATE_ARG = SYSTEM_UPDATE_ARG + "NonBlocking";
  public static final Set<String> SYSTEM_UPDATE_ARGS =
      Set.of(SYSTEM_UPDATE_ARG, BLOCKING_SYSTEM_UPDATE_ARG, NONBLOCKING_SYSTEM_UPDATE_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs().stream()
        .filter(Objects::nonNull)
        .anyMatch(SYSTEM_UPDATE_ARGS::contains);
  }

  public static class BlockingSystemUpdateCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return context
          .getBeanFactory()
          .getBean(ApplicationArguments.class)
          .getNonOptionArgs()
          .stream()
          .anyMatch(arg -> SYSTEM_UPDATE_ARG.equals(arg) || BLOCKING_SYSTEM_UPDATE_ARG.equals(arg));
    }
  }

  public static class NonBlockingSystemUpdateCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      return context
          .getBeanFactory()
          .getBean(ApplicationArguments.class)
          .getNonOptionArgs()
          .stream()
          .anyMatch(
              arg -> SYSTEM_UPDATE_ARG.equals(arg) || NONBLOCKING_SYSTEM_UPDATE_ARG.equals(arg));
    }
  }
}
