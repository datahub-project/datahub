package com.linkedin.datahub.upgrade.conditions;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class SqlSetupCondition implements Condition {
  public static final String SQL_SETUP_ARG = "SqlSetup";
  public static final Set<String> SQL_SETUP_ARGS = Set.of(SQL_SETUP_ARG);
  public static final String SQL_SETUP_ENABLED_ENV = "DATAHUB_SQL_SETUP_ENABLED";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return false;
    }
    return nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(SQL_SETUP_ARGS::contains);
  }

  public static boolean isSqlSetupEnabled() {
    String sqlSetupEnabled = System.getenv(SQL_SETUP_ENABLED_ENV);
    return "true".equalsIgnoreCase(sqlSetupEnabled);
  }
}
