package com.linkedin.datahub.upgrade.conditions;

import org.springframework.boot.autoconfigure.condition.AllNestedConditions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.ConfigurationCondition;

/**
 * LoadIndices upgrade only applies when the CLI requests it and Elasticsearch is enabled (the
 * upgrade bulk-indexes into OpenSearch/Elasticsearch).
 *
 * <p>Uses {@link ConfigurationCondition.ConfigurationPhase#PARSE_CONFIGURATION} so that {@code
 * LoadIndicesUpgradeConfig} is fully skipped (no {@code @Import}/{@code @ComponentScan} processing)
 * when the gate is false. With {@code REGISTER_BEAN}, the config class is still parsed — its
 * component scans pull {@code SystemOperationContextFactory} into other upgrade contexts (e.g.
 * SqlSetup) that don't carry the elasticsearch dependencies it needs.
 */
public class LoadIndicesWithElasticsearchEnabledCondition extends AllNestedConditions {

  public LoadIndicesWithElasticsearchEnabledCondition() {
    super(ConfigurationCondition.ConfigurationPhase.PARSE_CONFIGURATION);
  }

  @Conditional(LoadIndicesCondition.class)
  static class OnLoadIndicesRequested {}

  @ConditionalOnProperty(
      prefix = "elasticsearch",
      name = "enabled",
      havingValue = "true",
      matchIfMissing = true)
  static class OnElasticsearchEnabled {}
}
