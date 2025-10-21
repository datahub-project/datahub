package com.linkedin.datahub.upgrade.conditions;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Spring condition that determines whether SQL setup functionality should be enabled. This
 * condition checks for the presence of "SqlSetup" in the application's non-option arguments to
 * conditionally enable SQL setup related beans and configurations.
 */
public class SqlSetupCondition implements Condition {

  /** The command line argument that enables SQL setup functionality. */
  public static final String SQL_SETUP_ARG = "SqlSetup";

  /** Set containing all valid SQL setup arguments. */
  public static final Set<String> SQL_SETUP_ARGS = Set.of(SQL_SETUP_ARG);

  /** Environment variable name that can enable SQL setup functionality. */
  public static final String SQL_SETUP_ENABLED_ENV = "DATAHUB_SQL_SETUP_ENABLED";

  /**
   * Determines if the SQL setup condition matches based on application arguments. This method
   * checks if "SqlSetup" is present in the non-option arguments passed to the application.
   *
   * @param context the condition context providing access to Spring beans and environment
   * @param metadata metadata about the annotated type
   * @return true if "SqlSetup" is found in the non-option arguments, false otherwise
   */
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return false;
    }
    return nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(SQL_SETUP_ARGS::contains);
  }

  /**
   * Checks if SQL setup is enabled via environment variable. This method provides an alternative
   * way to enable SQL setup functionality by setting the DATAHUB_SQL_SETUP_ENABLED environment
   * variable to "true".
   *
   * @return true if the DATAHUB_SQL_SETUP_ENABLED environment variable is set to "true"
   *     (case-insensitive), false otherwise
   */
  public static boolean isSqlSetupEnabled() {
    String sqlSetupEnabled = System.getenv(SQL_SETUP_ENABLED_ENV);
    return "true".equalsIgnoreCase(sqlSetupEnabled);
  }
}
