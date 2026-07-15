package com.linkedin.datahub.upgrade.conditions;

import static com.linkedin.datahub.upgrade.cleanup.CleanupCondition.CLEANUP_ARG;
import static com.linkedin.datahub.upgrade.conditions.LoadIndicesCondition.LOAD_INDICES_ARG;
import static com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ARG;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class GeneralUpgradeCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    List<String> nonOptionArgs =
        context.getBeanFactory().getBean(ApplicationArguments.class).getNonOptionArgs();
    if (nonOptionArgs == null) {
      return true;
    }
    boolean elasticsearchIntegrationEnabled =
        Optional.ofNullable(
                context.getEnvironment().getProperty("elasticsearch.enabled", Boolean.class))
            .orElse(true);

    /*
     LoadIndices uses a slim UpgradeConfiguration only when Elasticsearch integration is enabled.
     With elasticsearch.enabled=false, ignore LoadIndices for this gate so {@link GeneralUpgradeConfiguration}
     still loads (UpgradeCli warns that LoadIndices beans are unavailable).
    */
    return !(nonOptionArgs.stream()
        .filter(Objects::nonNull)
        .anyMatch(
            arg ->
                SQL_SETUP_ARG.equals(arg)
                    || CLEANUP_ARG.equals(arg)
                    || (LOAD_INDICES_ARG.equals(arg) && elasticsearchIntegrationEnabled)));
  }
}
