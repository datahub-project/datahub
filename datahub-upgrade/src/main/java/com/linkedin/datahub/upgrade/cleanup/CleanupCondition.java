package com.linkedin.datahub.upgrade.cleanup;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Spring condition that matches when the CLI arguments contain {@code Cleanup}. This ensures the
 * cleanup-specific Spring configuration is only loaded for the cleanup upgrade path.
 */
public class CleanupCondition implements Condition {
  public static final String CLEANUP_ARG = "Cleanup";
  public static final Set<String> CLEANUP_ARGS = Set.of(CLEANUP_ARG);

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return false;
    }
    return nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(CLEANUP_ARGS::contains);
  }
}
