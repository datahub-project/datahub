package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.conditions.GeneralUpgradeCondition;
import com.linkedin.datahub.upgrade.conditions.LoadIndicesCondition;
import com.linkedin.datahub.upgrade.conditions.SqlSetupCondition;
import com.linkedin.datahub.upgrade.config.GeneralUpgradeConfiguration;
import com.linkedin.datahub.upgrade.loadindices.LoadIndicesUpgradeConfig;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupUpgradeConfig;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Configuration selector that chooses the appropriate upgrade configuration based on command-line
 * arguments.
 */
@Configuration
public class UpgradeConfigurationSelector {

  /** Configuration for LoadIndices upgrade - excludes Kafka components */
  @Configuration
  @Conditional(LoadIndicesCondition.class)
  @Import(LoadIndicesUpgradeConfig.class)
  public static class LoadIndicesConfiguration {}

  /** Configuration for SqlSetup upgrade - excludes Kafka components */
  @Configuration
  @Conditional(SqlSetupCondition.class)
  @Import(SqlSetupUpgradeConfig.class)
  public static class SqlSetupConfiguration {}

  /** Configuration for general upgrades - includes all components */
  @Configuration
  @Conditional(GeneralUpgradeCondition.class)
  @Import(GeneralUpgradeConfiguration.class)
  public static class GeneralConfiguration {}
}
