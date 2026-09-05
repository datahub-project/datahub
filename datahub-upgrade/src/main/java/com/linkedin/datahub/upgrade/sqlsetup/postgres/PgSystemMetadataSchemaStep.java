package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgSystemMetadataSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pgsystemmetadata.PgSystemMetadataSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pgsystemmetadata.PgSystemMetadataSqlMigrationTokens;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PgSystemMetadataSchemaStep implements UpgradeStep {

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgSystemMetadataSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgSystemMetadata schema...");
        PgSystemMetadataSetupOptions options = postgresProperties.buildPgSystemMetadataOptions();
        if (options == null) {
          String msg = "pgSystemMetadata is enabled but PgSystemMetadataSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        try (Connection connection =
            PgSystemMetadataStoreConnections.open(options, server, postgresProperties)) {
          connection.setAutoCommit(true);
          PgSystemMetadataSqlMigrationTokens tokens =
              PgSystemMetadataSqlMigrationTokens.builder()
                  .tableName(options.getTableName())
                  .build();
          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection, PgSystemMetadataSqlMigrationModules.from(options, tokens));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }
        }
        context.report().addLine("pgSystemMetadata schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgSystemMetadataSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgSystemMetadataSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }
}
