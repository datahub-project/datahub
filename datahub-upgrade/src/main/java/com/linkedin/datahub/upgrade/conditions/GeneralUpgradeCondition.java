package com.linkedin.datahub.upgrade.conditions;

import static com.linkedin.datahub.upgrade.cleanup.CleanupCondition.CLEANUP_ARG;
import static com.linkedin.datahub.upgrade.conditions.LoadIndicesCondition.LOAD_INDICES_ARG;
import static com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ARG;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class GeneralUpgradeCondition implements Condition {
  public static final Set<String> EXCLUDED_ARGS =
      Set.of(LOAD_INDICES_ARG, SQL_SETUP_ARG, CLEANUP_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return true;
    }
    return !nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(EXCLUDED_ARGS::contains);
  }
}
