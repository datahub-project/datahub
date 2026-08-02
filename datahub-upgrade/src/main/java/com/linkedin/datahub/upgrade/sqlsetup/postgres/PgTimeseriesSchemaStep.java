package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.PostgresPartmanSqlSetupSupport;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupExtensions;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pgtimeseries.PgTimeseriesSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pgtimeseries.PgTimeseriesSqlMigrationTokens;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PgTimeseriesSchemaStep implements UpgradeStep {

  private static final Set<String> PGTIMESERIES_PARTMAN_EXTENSIONS = Set.of("pg_partman");
  private static final Set<String> PGTIMESERIES_CRON_EXTENSIONS = Set.of("pg_cron");

  /** Soft-skip count when cron was requested but not registered (ops metric). */
  static final AtomicLong CRON_REGISTRATION_SKIPPED = new AtomicLong();

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgTimeseriesSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgTimeseries schema...");
        PgTimeseriesSetupOptions o = postgresProperties.buildPgTimeseriesOptions();
        if (o == null) {
          String msg = "pgTimeseries is enabled but PgTimeseriesSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        String schema = o.getSchema();
        String cronSchema = postgresProperties.normalizedPgCronSchema();
        String tablePrefix = o.getTablePrefix();

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);

          PostgresSqlSetupExtensions.maybeCreateExtension(
              connection, "pg_partman", true, PGTIMESERIES_PARTMAN_EXTENSIONS);
          if (!PostgresSqlSetupExtensions.isExtensionInstalled(connection, "pg_partman")) {
            String msg =
                "pgTimeseries SqlSetup requires pg_partman but it is not installed. "
                    + "Install the extension (it must appear in pg_available_extensions).";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          String partmanExtensionSchema =
              PostgresPartmanSqlSetupSupport.resolvePgPartmanExtensionSchema(connection);
          if (partmanExtensionSchema == null || partmanExtensionSchema.isBlank()) {
            String msg =
                "pg_partman is installed but its extension schema could not be read from"
                    + " pg_extension / pg_namespace.";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          PgTimeseriesSqlMigrationTokens tokens =
              PgTimeseriesSqlMigrationTokens.builder()
                  .tablePrefix(tablePrefix)
                  .partmanParentQualified(schema + "." + tablePrefix + "_aspect")
                  .partmanInterval(
                      PostgresPartmanSqlSetupSupport.sanitizePartmanIntervalLiteral(
                          o.getPartmanPartitionInterval()))
                  .partmanPremake(Integer.toString(o.getPartmanPremake()))
                  .partmanForceOverwrite(o.isForceOverwritePartmanConfig() ? "true" : "false")
                  .build();

          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection, PgTimeseriesSqlMigrationModules.from(o, tokens));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }

          String retentionUpdateSql;
          if (o.getRetentionMaxAgeSeconds() > 0) {
            String partmanRetentionIntervalText =
                PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(
                    o.getRetentionMaxAgeSeconds(), 0, o.getPartmanPartitionInterval());
            retentionUpdateSql =
                PostgresPartmanSqlSetupSupport.partmanRetentionUpdateSql(
                    partmanExtensionSchema,
                    schema,
                    partmanRetentionIntervalText,
                    tablePrefix + "_aspect");
          } else {
            retentionUpdateSql =
                PostgresPartmanSqlSetupSupport.partmanRetentionClearSql(
                    partmanExtensionSchema, schema, tablePrefix + "_aspect");
          }
          if (!retentionUpdateSql.isEmpty()) {
            PostgresSqlUtils.executeSql(connection, retentionUpdateSql);
          }

          if (o.isMaintenanceCronEnabled()) {
            String jobDb = connection.getCatalog();
            try (Connection cronConn = PgCronAdminConnections.open(postgresProperties)) {
              PostgresSqlSetupExtensions.maybeCreateExtension(
                  cronConn, "pg_cron", true, PGTIMESERIES_CRON_EXTENSIONS);
              @Nullable
              String cronSkipReason =
                  registerTimeseriesPartmanCronJob(
                      cronConn,
                      cronSchema,
                      schema,
                      o.getMaintenanceIntervalSeconds(),
                      tablePrefix,
                      jobDb,
                      partmanExtensionSchema);
              if (cronSkipReason != null) {
                CRON_REGISTRATION_SKIPPED.incrementAndGet();
                context
                    .report()
                    .addLine(
                        "WARN: pgTimeseries maintenance cron was requested but not registered: "
                            + cronSkipReason
                            + " Schedule partman.run_maintenance externally or install/fix pg_cron.");
              }
            }
          }
        }

        context.report().addLine("pgTimeseries schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgTimeseriesSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgTimeseriesSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Registers the partman maintenance cron job, or returns a human-readable skip reason (soft-skip;
   * caller still SUCCEEDED).
   */
  @Nullable
  private static String registerTimeseriesPartmanCronJob(
      Connection cronConnection,
      String cronSchema,
      String applicationSchema,
      int intervalSeconds,
      String tablePrefix,
      String jobTargetDatabase,
      String partmanExtensionSchema)
      throws SQLException {
    if (jobTargetDatabase == null || jobTargetDatabase.isBlank()) {
      String reason =
          "JDBC catalog (database name) is empty; fix the entity store JDBC URL / connection.";
      log.error("Cannot register pgTimeseries pg_cron job: {}", reason);
      return reason;
    }
    String jobName =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGTIMESERIES_CRON_ROLE,
            jobTargetDatabase,
            applicationSchema,
            tablePrefix);
    String schedule = PostgresPartmanSqlSetupSupport.toPgCronSchedule(intervalSeconds);
    String parentTable = applicationSchema + "." + tablePrefix + "_aspect";
    String maintCall =
        "SELECT "
            + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
            + ".run_maintenance('"
            + PgCronMaintenance.escapeSqlStringLiteral(parentTable)
            + "')";
    if (!PgCronMaintenance.isExtensionInstalled(cronConnection, "pg_cron")) {
      String reason =
          "pg_cron is not installed (set postgres.pgTimeseries.maintenance.cronEnabled=false "
              + "or install pg_cron so it appears in pg_available_extensions).";
      log.warn("Skipping in-database schedule for job {}: {}", jobName, reason);
      return reason;
    }
    PgCronMaintenance.replaceCronJobInDatabase(
        cronConnection, cronSchema, jobName, schedule, maintCall, jobTargetDatabase);
    log.info(
        "Registered pg_cron job {} with schedule '{}' for {} (target database {})",
        jobName,
        schedule,
        maintCall,
        jobTargetDatabase);
    return null;
  }
}
