package com.linkedin.datahub.upgrade;

import com.linkedin.datahub.upgrade.conditions.SqlSetupCondition;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Main application entry point for DataHub upgrade operations.
 *
 * <p>This application provides a comprehensive upgrade framework for DataHub that supports multiple
 * types of upgrade operations:
 *
 * <h3>Available Upgrade Types:</h3>
 *
 * <ul>
 *   <li><strong>SqlSetup</strong> - Initializes database schema and tables required for DataHub
 *       operation
 *   <li><strong>LoadIndices</strong> - Loads and initializes search indices for entity discovery
 *   <li><strong>RestoreIndices</strong> - Restores search indices from backup or rebuilds them from
 *       database
 *   <li><strong>RestoreBackup</strong> - Restores DataHub data from backup files
 *   <li><strong>RemoveUnknownAspects</strong> - Cleans up unknown or invalid aspect data
 *   <li><strong>SystemUpdate</strong> - Performs comprehensive system updates including blocking
 *       and non-blocking operations
 *   <li><strong>SystemUpdateBlocking</strong> - Executes blocking system updates that require
 *       service downtime
 *   <li><strong>SystemUpdateNonBlocking</strong> - Executes non-blocking system updates that can
 *       run while services are active. Use -n to run a specific upgrade by name.
 *   <li><strong>SystemUpdateCron</strong> - Scheduled system updates for maintenance operations
 *   <li><strong>ReindexDebug</strong> - Debug tool for reindexing operations
 * </ul>
 *
 * <h3>Command Line Usage:</h3>
 *
 * <pre>
 * java -jar datahub-upgrade.jar -u SqlSetup
 * java -jar datahub-upgrade.jar -u SystemUpdate
 * java -jar datahub-upgrade.jar -u RestoreIndices -a batchSize=1000
 * java -jar datahub-upgrade.jar -u SystemUpdateNonBlocking -n BackfillBrowsePathsV2
 * </pre>
 *
 * <h3>Configuration:</h3>
 *
 * <p>Upgrade configurations are selected conditionally based on the upgrade type: - LoadIndices and
 * SqlSetup use minimal configurations (exclude Kafka components) - General upgrades use full
 * configuration with all DataHub components
 *
 * <h3>Error Handling:</h3>
 *
 * <p>The application exits with code 1 on upgrade failure, code 0 on success.
 */
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@Slf4j
public class UpgradeCliApplication {

  public static void main(String[] args) {
    log.info("Starting UpgradeCli Application...");

    if (SqlSetupCondition.isSqlSetupEnabled()) {
      log.info("Starting SQL Setup Bootstrap Application...");

      boolean sqlSetupSuccess = runSqlSetup();
      if (!sqlSetupSuccess) {
        log.error("SQL Setup failed, exiting");
        System.exit(1);
      }

      log.info("SQL Setup completed successfully, proceeding to system-update...");
    } else {
      log.info("SQL setup disabled (SQL_SETUP_ENABLED=false), skipping...");
    }

    // System Update (always run with original arguments)
    new SpringApplicationBuilder(UpgradeConfigurationSelector.class, UpgradeCli.class)
        .web(WebApplicationType.NONE)
        .run(args);
  }

  /**
   * Run SQL setup upgrade without using UpgradeCli. This prevents the application from exiting
   * after SQL setup completion.
   *
   * @return true if SQL setup succeeded, false otherwise
   */
  private static boolean runSqlSetup() {
    ConfigurableApplicationContext sqlSetupContext = null;
    try {
      log.info("Creating SQL setup context...");
      String[] sqlSetupArgs = {"-u", "SqlSetup"};
      sqlSetupContext =
          new SpringApplicationBuilder(UpgradeConfigurationSelector.class)
              .web(WebApplicationType.NONE)
              .run(sqlSetupArgs);

      log.info("Executing SQL setup upgrade...");
      OperationContext systemOperationContext =
          sqlSetupContext.getBean("systemOperationContext", OperationContext.class);
      SqlSetup sqlSetup = sqlSetupContext.getBean(SqlSetup.class);

      DefaultUpgradeManager upgradeManager = new DefaultUpgradeManager();
      upgradeManager.register(sqlSetup);

      var result = upgradeManager.execute(systemOperationContext, sqlSetup.id(), List.of());
      boolean success = result.result() == DataHubUpgradeState.SUCCEEDED;

      if (success) {
        log.info("SQL setup upgrade completed successfully");
      } else {
        log.error("SQL setup upgrade failed: {}", result.report());
      }

      return success;

    } catch (Exception e) {
      log.error("Error during SQL setup upgrade", e);
      return false;
    } finally {
      if (sqlSetupContext != null) {
        log.info("Closing SQL setup context...");
        sqlSetupContext.close();
        log.info("SQL setup context closed");
      }
    }
  }
}
