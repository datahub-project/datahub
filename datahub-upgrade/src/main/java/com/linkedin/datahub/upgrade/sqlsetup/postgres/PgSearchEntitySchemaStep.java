package com.linkedin.datahub.upgrade.sqlsetup.postgres;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.PgSearchEntitySetupOptions;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupExtensions;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlMigrationRunner;
import com.linkedin.metadata.sqlsetup.postgres.migration.PostgresSqlUtils;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationResult;
import com.linkedin.metadata.sqlsetup.postgres.pgsearch.PgSearchEntitySqlMigrationModules;
import com.linkedin.metadata.sqlsetup.postgres.pgsearch.PgSearchEntitySqlSetupSupport;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Applies PostgreSQL DDL for the future hybrid entity search store (lexical + optional pgvector).
 *
 * <p>Gated by {@code postgres.pgSearch.entity.enabled}. When {@code
 * postgres.pgSearch.entity.vector.enabled} is true, requires the {@code vector} extension (install
 * {@code postgresql-${PG_MAJOR}-pgvector} in the image; see {@code docker/postgres/Dockerfile}).
 * After running extension DDL, the step asserts {@code vector} is present in {@code pg_extension}
 * so swallowed {@code CREATE EXTENSION} failures do not report success.
 *
 * <p>No data is written to {@code group_registry}; rows are inserted in blocking {@code
 * SystemUpdate} by {@link
 * com.linkedin.datahub.upgrade.system.postgres.PgSearchEntitySearchGroupRegistrySeedStep} from the
 * entity registry (after this DDL creates empty tables).
 */
@Slf4j
@RequiredArgsConstructor
public class PgSearchEntitySchemaStep implements UpgradeStep {

  private final Database server;
  private final PostgresSqlSetupProperties postgresProperties;

  @Override
  public String id() {
    return "PgSearchEntitySchemaStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Applying PostgreSQL pgSearch entity schema...");
        PgSearchEntitySetupOptions options = postgresProperties.buildPgSearchEntityOptions();
        if (options == null) {
          String msg = "pgSearch entity is enabled but PgSearchEntitySetupOptions is null.";
          log.error(msg);
          context.report().addLine(msg);
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        try (Connection connection = server.dataSource().getConnection()) {
          connection.setAutoCommit(true);

          if (options.isVectorEnabled()) {
            if (!PostgresSqlSetupExtensions.isExtensionAvailable(connection, "vector")) {
              String msg =
                  "pgvector is not available in this PostgreSQL server (missing OS package). "
                      + "Build the image from docker/postgres/Dockerfile (postgresql-${PG_MAJOR}-pgvector) "
                      + "and set DATAHUB_POSTGRES_IMAGE, or set postgres.pgSearch.entity.vector.enabled=false "
                      + "(env DATAHUB_PGSEARCH_ENTITY_VECTOR_ENABLED).";
              log.error(msg);
              context.report().addLine(msg);
              return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
            }
          }

          applySessionVariables(connection, options);

          SqlMigrationResult migrationResult =
              PostgresSqlMigrationRunner.migrate(
                  connection,
                  PgSearchEntitySqlMigrationModules.from(
                      options,
                      PgSearchEntitySqlSetupSupport.buildMigrationTokens(postgresProperties)));
          for (String applied : migrationResult.getApplied()) {
            context.report().addLine("Applied migration: " + applied);
          }

          if (options.isVectorEnabled()
              && !PostgresSqlSetupExtensions.isExtensionInstalled(connection, "vector")) {
            String msg =
                "The vector extension is not installed after CREATE EXTENSION (check server logs "
                    + "for NOTICE lines from V001__extensions.sql). "
                    + "Typical causes: insufficient privileges to run CREATE EXTENSION, or "
                    + "CREATE EXTENSION failed despite the package being listed in pg_available_extensions. "
                    + "Use a role allowed to create extensions on your Postgres deployment, or set "
                    + "postgres.pgSearch.entity.vector.enabled=false (env DATAHUB_PGSEARCH_ENTITY_VECTOR_ENABLED).";
            log.error(msg);
            context.report().addLine(msg);
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }

          String schema = options.getSchema();
          PostgresSqlUtils.executeSql(
              connection,
              "COMMENT ON SCHEMA "
                  + PostgresSqlUtils.quotePgIdentifier(schema)
                  + " IS 'DataHub optional PostgreSQL features (SqlSetup); includes pgSearch entity search objects.'");
        }

        context.report().addLine("pgSearch entity schema applied successfully.");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (SqlMigrationException e) {
        log.error("PgSearchEntitySchemaStep migration failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      } catch (Exception e) {
        log.error("PgSearchEntitySchemaStep failed", e);
        context.report().addLine(String.format("Error: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private static void applySessionVariables(
      Connection connection, PgSearchEntitySetupOptions options) throws SQLException {
    String lang = options.getFulltextDefaultLanguage();
    if (lang == null || lang.isBlank()) {
      lang = "english";
    }
    String escaped = lang.replace("'", "''");
    try (Statement st = connection.createStatement()) {
      st.execute("SET datahub.pgsearch_entity_fulltext_language = '" + escaped + "'");
    }
  }
}
