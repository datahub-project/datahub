package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgGraphSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pgrouting.PgGraphSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pgrouting.PgGraphSqlMigrationTokens;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PgRoutingGraphSchemaStep implements UpgradeStep {

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgRoutingGraphSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgRouting graph schema...");
        PgGraphSetupOptions options = postgresProperties.buildPgGraphOptions();
        if (options == null) {
          String msg = "pgGraph is enabled but PgGraphSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        PgGraphSqlMigrationTokens tokens =
            PgGraphSqlMigrationTokens.builder().tablePrefix(options.getTablePrefix()).build();

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);

          if (!areGraphExtensionsPackaged(connection)) {
            String msg =
                "PostGIS/pgRouting are not available in this PostgreSQL server "
                    + "(missing OS packages). Build the image from docker/postgres/Dockerfile and set "
                    + "DATAHUB_POSTGRES_IMAGE, or disable postgres.pgGraph.enabled "
                    + "(env DATAHUB_PGGRAPH_ENABLED).";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          setPartitionCount(connection, options.getPartitionCount());

          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection, PgGraphSqlMigrationModules.from(options, tokens));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }
        }

        context.report().addLine("pgRouting graph schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgRoutingGraphSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgRoutingGraphSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private static boolean areGraphExtensionsPackaged(Connection connection) throws SQLException {
    try (Statement st = connection.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT COUNT(*) FROM pg_available_extensions WHERE name IN ('postgis','pgrouting')")) {
      return rs.next() && rs.getInt(1) >= 2;
    }
  }

  private static void setPartitionCount(Connection connection, int partitionCount)
      throws SQLException {
    try (Statement st = connection.createStatement()) {
      st.execute("SET datahub.pgrouting_partition_count = '" + partitionCount + "'");
    }
  }
}
