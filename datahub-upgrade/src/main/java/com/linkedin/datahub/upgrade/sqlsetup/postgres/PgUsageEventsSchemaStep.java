package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgUsageEventsSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.usage_events.PgUsageEventsSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.usage_events.PgUsageEventsSqlMigrationTokens;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PgUsageEventsSchemaStep implements UpgradeStep {

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgUsageEventsSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL usage events schema...");
        PgUsageEventsSetupOptions options = postgresProperties.buildPgUsageEventsOptions();
        if (options == null) {
          String msg = "pgUsageEvents is enabled but PgUsageEventsSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        PgUsageEventsSqlMigrationTokens tokens =
            PgUsageEventsSqlMigrationTokens.builder()
                .parentTableName(options.getParentTableName())
                .build();

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);
          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection, PgUsageEventsSqlMigrationModules.from(options, tokens));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }
        }

        context.report().addLine("Usage events schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgUsageEventsSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgUsageEventsSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }
}
