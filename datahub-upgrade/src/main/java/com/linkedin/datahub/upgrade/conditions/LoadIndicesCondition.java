package com.linkedin.datahub.upgrade.conditions;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class LoadIndicesCondition implements Condition {
  public static final String LOAD_INDICES_ARG = "LoadIndices";

  /**
   * Single source of truth for "is the operator asking us to run LoadIndices?". Used by both this
   * Spring {@link Condition} and {@code UpgradeCli} (for the not-wired warning) so the arg name
   * lives in exactly one place.
   */
  public static boolean isLoadIndicesRequested(@Nullable List<String> nonOptionArgs) {
    if (nonOptionArgs == null) {
      return false;
    }
    return nonOptionArgs.stream().filter(Objects::nonNull).anyMatch(LOAD_INDICES_ARG::equals);
  }

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return isLoadIndicesRequested(
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs());
  }
}
