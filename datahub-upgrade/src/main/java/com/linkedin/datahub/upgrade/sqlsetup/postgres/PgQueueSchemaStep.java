package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pgqueue.PgQueueSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pgqueue.PgQueueSqlMigrationTokens;
import com.linkedin.metadata.sqlsetup.postgres.pgqueue.PgQueueSqlSetupSupport;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies PostgreSQL DDL for the DataHub native queue store via {@link PostgresSqlMigrationRunner}
 * (requires {@code pg_partman}; optional {@code pg_cron} when maintenance cron is enabled).
 */
@Slf4j
@RequiredArgsConstructor
public class PgQueueSchemaStep implements UpgradeStep {

  private static final Set<String> PGQUEUE_PARTMAN_EXTENSIONS = Set.of("pg_partman");
  private static final Set<String> PGQUEUE_CRON_EXTENSIONS = Set.of("pg_cron");

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgQueueSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgQueue schema...");
        PgQueueSetupOptions q = postgresProperties.buildPgQueueOptions();
        if (q == null) {
          String msg = "pgQueue is enabled but PgQueueSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        String schema = q.getSchema();
        String cronSchema = postgresProperties.normalizedPgCronSchema();

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);

          maybeCreateExtension(connection, "pg_partman", true, PGQUEUE_PARTMAN_EXTENSIONS);
          if (!isExtensionInstalled(connection, "pg_partman")) {
            String msg =
                "pgQueue SqlSetup requires pg_partman but it is not installed. "
                    + "Install the extension (it must appear in pg_available_extensions).";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          String partmanExtensionSchema =
              PgQueueSqlSetupSupport.resolvePgPartmanExtensionSchema(connection);
          if (partmanExtensionSchema == null || partmanExtensionSchema.isBlank()) {
            String msg =
                "pg_partman is installed but its extension schema could not be read from"
                    + " pg_extension / pg_namespace.";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          String tablePrefix = q.getTablePrefix();

          PgQueueSqlMigrationTokens tokens =
              PgQueueSqlMigrationTokens.builder()
                  .quotedSchema(PostgresSqlUtils.quotePgIdentifier(schema))
                  .tablePrefix(tablePrefix)
                  .batchDeleteLimit(Integer.toString(q.getMaintenanceBatchDeleteLimit()))
                  .partmanParentQualified(schema + "." + tablePrefix + "_message")
                  .partmanInterval(
                      PgQueueSqlSetupSupport.sanitizePartmanIntervalLiteral(
                          q.getPartmanPartitionInterval()))
                  .partmanPremake(Integer.toString(q.getPartmanPremake()))
                  .retentionPartmanTail(
                      PgQueueSqlSetupSupport.buildRetentionPartmanTail(
                          partmanExtensionSchema, schema, tablePrefix))
                  .build();

          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection, PgQueueSqlMigrationModules.from(q, tokens));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }

          int maxTopicRetention =
              PgQueueSqlSetupSupport.queryMaxTopicRetentionMaxAgeSeconds(
                  connection, schema, tablePrefix);
          @Nullable
          String partmanRetentionIntervalText =
              PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(
                  q.getTopicDefaultRetentionMaxAgeSeconds(),
                  maxTopicRetention,
                  q.getPartmanPartitionInterval());

          PgQueueSqlSetupSupport.upsertTopicCatalog(connection, q);

          String retentionUpdateSql =
              PgQueueSqlSetupSupport.partmanRetentionUpdateSql(
                  partmanExtensionSchema, schema, partmanRetentionIntervalText, tablePrefix);
          if (!retentionUpdateSql.isEmpty()) {
            PostgresSqlUtils.executeSql(connection, retentionUpdateSql);
          }

          if (q.isMaintenanceCronEnabled()) {
            String jobDb = connection.getCatalog();
            try (Connection cronConn = PgCronAdminConnections.open(postgresProperties)) {
              maybeCreateExtension(cronConn, "pg_cron", true, PGQUEUE_CRON_EXTENSIONS);
              registerQueueRetentionCronJob(
                  cronConn,
                  cronSchema,
                  schema,
                  tablePrefix,
                  q.getMaintenanceIntervalSeconds(),
                  jobDb);
            }
          }
        }

        context.report().addLine("pgQueue schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgQueueSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgQueueSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private static void maybeCreateExtension(
      Connection connection, String extensionName, boolean want, Set<String> allowedNames)
      throws SQLException {
    if (!want) {
      return;
    }
    if (!allowedNames.contains(extensionName)) {
      throw new IllegalArgumentException("Unsupported extension name: " + extensionName);
    }
    if (!isExtensionAvailable(connection, extensionName)) {
      log.warn(
          "Extension {} is not listed in pg_available_extensions; skipping CREATE EXTENSION.",
          extensionName);
      return;
    }
    try (Statement st = connection.createStatement()) {
      st.execute("CREATE EXTENSION IF NOT EXISTS " + extensionName);
      log.info("CREATE EXTENSION IF NOT EXISTS {} attempted.", extensionName);
    } catch (SQLException e) {
      log.warn(
          "CREATE EXTENSION {} skipped or failed (non-fatal for SqlSetup): {}",
          extensionName,
          e.getMessage());
    }
  }

  private static boolean isExtensionAvailable(Connection connection, String extensionName)
      throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        var rs =
            st.executeQuery(
                "SELECT 1 FROM pg_available_extensions WHERE name = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }

  private static boolean isExtensionInstalled(Connection connection, String extensionName)
      throws SQLException {
    String safe = extensionName.replace("'", "''");
    try (Statement st = connection.createStatement();
        var rs =
            st.executeQuery("SELECT 1 FROM pg_extension WHERE extname = '" + safe + "' LIMIT 1")) {
      return rs.next();
    }
  }

  private static void registerQueueRetentionCronJob(
      Connection cronConnection,
      String cronSchema,
      String applicationSchema,
      String tablePrefix,
      int intervalSeconds,
      String jobTargetDatabase)
      throws SQLException {
    if (jobTargetDatabase == null || jobTargetDatabase.isBlank()) {
      log.error(
          "Cannot register pgQueue pg_cron job: JDBC catalog (database name) is empty; "
              + "fix the entity store JDBC URL / connection.");
      return;
    }
    String jobName =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGQUEUE_CRON_ROLE, jobTargetDatabase, applicationSchema, tablePrefix);
    String schedule = toPgCronSchedule(intervalSeconds);
    if (!PgCronMaintenance.isExtensionInstalled(cronConnection, "pg_cron")) {
      log.warn(
          "pg_cron is not installed; skipping in-database schedule for job {}. "
              + "Use postgres.pgQueue.maintenance.cronEnabled=false or install pg_cron so it appears in pg_available_extensions.",
          jobName);
      return;
    }
    String command = "SELECT " + applicationSchema + "." + tablePrefix + "_apply_retention()";
    PgCronMaintenance.replaceCronJobInDatabase(
        cronConnection, cronSchema, jobName, schedule, command, jobTargetDatabase);
    log.info(
        "Registered pg_cron job {} with schedule '{}' for {} (target database {})",
        jobName,
        schedule,
        command,
        jobTargetDatabase);
  }

  /** Maps intervalSeconds to a pg_cron schedule (minute/hour granularity). */
  public static String toPgCronSchedule(int intervalSeconds) {
    int sec = Math.max(60, intervalSeconds);
    if (sec % 86400 == 0) {
      int days = sec / 86400;
      days = Math.max(1, Math.min(31, days));
      return days == 1 ? "0 0 * * *" : ("0 0 */" + days + " * *");
    }
    if (sec % 3600 == 0) {
      int hours = Math.max(1, Math.min(23, sec / 3600));
      return "0 */" + hours + " * * *";
    }
    int minutes = Math.max(1, Math.min(59, sec / 60));
    return "*/" + minutes + " * * * *";
  }
}
