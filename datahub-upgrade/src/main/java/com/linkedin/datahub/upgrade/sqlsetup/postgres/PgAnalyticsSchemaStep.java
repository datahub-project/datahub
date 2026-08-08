package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgAnalyticsSetupOptions;
import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.PostgresPartmanSqlSetupSupport;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupExtensions;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pganalytics.PgAnalyticsSqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pganalytics.PgAnalyticsSqlMigrationTokens;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class PgAnalyticsSchemaStep implements UpgradeStep {

  private static final Set<String> PGANALYTICS_PARTMAN_EXTENSIONS = Set.of("pg_partman");
  private static final Set<String> PGANALYTICS_CRON_EXTENSIONS = Set.of("pg_cron");

  static final AtomicLong CRON_REGISTRATION_SKIPPED = new AtomicLong();

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgAnalyticsSchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgAnalytics schema...");
        PgAnalyticsSetupOptions registry = postgresProperties.buildPgAnalyticsOptions();
        if (registry == null) {
          String msg = "pgAnalytics is enabled but PgAnalyticsSetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
        String cronSchema = postgresProperties.normalizedPgCronSchema();

        for (PgAnalyticsStoreOptions store : registry.getStores().values()) {
          context.report().addLine("Applying pgAnalytics store '" + store.getName() + "'...");
          try (Connection connection =
              PgAnalyticsStoreConnections.open(store, server, postgresProperties)) {
            connection.setAutoCommit(true);
            applyStore(context, store, connection, cronSchema);
          }
        }

        context.report().addLine("pgAnalytics schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgAnalyticsSchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgAnalyticsSchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void applyStore(
      UpgradeContext context,
      PgAnalyticsStoreOptions store,
      Connection connection,
      String cronSchema)
      throws SQLException, SqlMigrationException {
    String schema = store.getSchema();
    String tablePrefix = store.getTablePrefix();

    PostgresSqlSetupExtensions.maybeCreateExtension(
        connection, "pg_partman", true, PGANALYTICS_PARTMAN_EXTENSIONS);
    if (!PostgresSqlSetupExtensions.isExtensionInstalled(connection, "pg_partman")) {
      throw new IllegalStateException(
          "pgAnalytics SqlSetup requires pg_partman but it is not installed on store '"
              + store.getName()
              + "'. Install the extension (it must appear in pg_available_extensions).");
    }

    String partmanExtensionSchema =
        PostgresPartmanSqlSetupSupport.resolvePgPartmanExtensionSchema(connection);
    if (partmanExtensionSchema == null || partmanExtensionSchema.isBlank()) {
      throw new IllegalStateException(
          "pg_partman is installed but its extension schema could not be read (store '"
              + store.getName()
              + "').");
    }

    PgAnalyticsSqlMigrationTokens tokens =
        PgAnalyticsSqlMigrationTokens.builder()
            .tablePrefix(tablePrefix)
            .partmanParentEvent(schema + "." + tablePrefix + "_event")
            .partmanParentRollup(schema + "." + tablePrefix + "_rollup")
            .partmanParentDistinct(schema + "." + tablePrefix + "_distinct_set")
            .partmanInterval(
                PostgresPartmanSqlSetupSupport.sanitizePartmanIntervalLiteral(
                    store.getPartmanPartitionInterval()))
            .partmanPremake(Integer.toString(store.getPartmanPremake()))
            .partmanForceOverwrite(store.isForceOverwritePartmanConfig() ? "true" : "false")
            .build();

    SqlMigrationResult migrationResult =
        PostgresSqlMigrationRunner.migrate(
            connection, PgAnalyticsSqlMigrationModules.from(store, tokens));
    for (String applied : migrationResult.getApplied()) {
      context.report().addLine("store=" + store.getName() + " Applied migration: " + applied);
    }

    applyPerParentRetention(
        connection,
        partmanExtensionSchema,
        schema,
        tablePrefix + "_event",
        store.getRawMaxAgeSeconds(),
        store.getPartmanPartitionInterval());
    applyPerParentRetention(
        connection,
        partmanExtensionSchema,
        schema,
        tablePrefix + "_rollup",
        Math.max(store.getHourlyMaxAgeSeconds(), store.getDailyMaxAgeSeconds()),
        store.getPartmanPartitionInterval());
    applyPerParentRetention(
        connection,
        partmanExtensionSchema,
        schema,
        tablePrefix + "_distinct_set",
        Math.max(store.getHourlyMaxAgeSeconds(), store.getDailyMaxAgeSeconds()),
        store.getPartmanPartitionInterval());

    if (store.isMaintenanceCronEnabled()) {
      String jobDb = connection.getCatalog();
      try (Connection cronConn = PgCronAdminConnections.open(postgresProperties)) {
        PostgresSqlSetupExtensions.maybeCreateExtension(
            cronConn, "pg_cron", true, PGANALYTICS_CRON_EXTENSIONS);
        for (String parentSuffix : List.of("_event", "_rollup", "_distinct_set")) {
          @Nullable
          String cronSkipReason =
              registerPartmanCronJob(
                  cronConn,
                  cronSchema,
                  schema,
                  store.getMaintenanceIntervalSeconds(),
                  tablePrefix + parentSuffix,
                  jobDb,
                  partmanExtensionSchema);
          if (cronSkipReason != null) {
            CRON_REGISTRATION_SKIPPED.incrementAndGet();
            context
                .report()
                .addLine(
                    "WARN: pgAnalytics store '"
                        + store.getName()
                        + "' maintenance cron was requested but not registered for "
                        + tablePrefix
                        + parentSuffix
                        + ": "
                        + cronSkipReason);
          }
        }
      }
    }
  }

  private static void applyPerParentRetention(
      Connection connection,
      String partmanExtensionSchema,
      String schema,
      String parentTable,
      int retentionMaxAgeSeconds,
      String partmanInterval)
      throws SQLException {
    String retentionUpdateSql;
    if (retentionMaxAgeSeconds > 0) {
      String partmanRetentionIntervalText =
          PostgresSqlSetupProperties.resolvePartmanPartitionRetentionIntervalText(
              retentionMaxAgeSeconds, 0, partmanInterval);
      retentionUpdateSql =
          PostgresPartmanSqlSetupSupport.partmanRetentionUpdateSql(
              partmanExtensionSchema, schema, partmanRetentionIntervalText, parentTable);
    } else {
      retentionUpdateSql =
          PostgresPartmanSqlSetupSupport.partmanRetentionClearSql(
              partmanExtensionSchema, schema, parentTable);
    }
    if (!retentionUpdateSql.isEmpty()) {
      PostgresSqlUtils.executeSql(connection, retentionUpdateSql);
    }
  }

  @Nullable
  private static String registerPartmanCronJob(
      Connection cronConnection,
      String cronSchema,
      String applicationSchema,
      int intervalSeconds,
      String tableName,
      String jobTargetDatabase,
      String partmanExtensionSchema)
      throws SQLException {
    if (jobTargetDatabase == null || jobTargetDatabase.isBlank()) {
      return "JDBC catalog (database name) is empty; fix the entity store JDBC URL / connection.";
    }
    String jobName =
        PgCronMaintenance.buildScopedCronJobName(
            PgCronMaintenance.PGANALYTICS_CRON_ROLE,
            jobTargetDatabase,
            applicationSchema,
            tableName);
    String schedule = PostgresPartmanSqlSetupSupport.toPgCronSchedule(intervalSeconds);
    String parentTable = applicationSchema + "." + tableName;
    String maintCall =
        "SELECT "
            + PostgresSqlUtils.quotePgIdentifier(partmanExtensionSchema)
            + ".run_maintenance('"
            + PgCronMaintenance.escapeSqlStringLiteral(parentTable)
            + "')";
    if (!PgCronMaintenance.isExtensionInstalled(cronConnection, "pg_cron")) {
      return "pg_cron is not installed (set postgres.pgAnalytics.maintenance.cronEnabled=false "
          + "or install pg_cron).";
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
