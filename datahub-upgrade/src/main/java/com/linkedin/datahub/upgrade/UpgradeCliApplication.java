package com.linkedin.datahub.upgrade;

import static com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.isSqlSetupEnabled;

import com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@Slf4j
public class UpgradeCliApplication {

  public static void main(String[] args) {
    log.info("Starting UpgradeCli Application...");

    if (isSqlSetupEnabled()) {
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
   * Manually run SQL setup upgrade without using UpgradeCli. This prevents the application from
   * exiting after SQL setup completion.
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
