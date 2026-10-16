package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.sqlsetup.postgres.PostgresSqlSetupSession;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateTablesStep implements UpgradeStep {

  private final Database server;
  private final SqlSetupArgs setupArgs;
  private final DatabaseOperations dbOps;

  public CreateTablesStep(final Database server, final SqlSetupArgs setupArgs) {
    this.server = server;
    this.setupArgs = setupArgs;
    this.dbOps = DatabaseOperations.create(setupArgs.getDbType());
  }

  @Override
  public String id() {
    return "CreateTablesStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        context.report().addLine("Creating database tables...");

        SqlSetupResult result = createTables(setupArgs);

        context.report().addLine(String.format("Tables created: %d", result.getTablesCreated()));
        context
            .report()
            .addLine(String.format("Execution time: %d ms", result.getExecutionTimeMs()));

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);

      } catch (Exception e) {
        log.error("Error during CreateTablesStep execution", e);
        context.report().addLine(String.format("Error during execution: %s", e.getMessage()));
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  SqlSetupResult createTables(SqlSetupArgs args) throws SQLException {
    SqlSetupResult result = new SqlSetupResult();
    long startTime = System.currentTimeMillis();

    // Create database if needed
    if (args.isCreateDatabase()) {
      try (Connection connection = server.dataSource().getConnection()) {
        dbOps.createDatabaseIfNotExists(args.getDatabaseName(), connection);
      }
    }

    // Select the database (MySQL only, PostgreSQL doesn't need this)
    try (Connection connection = server.dataSource().getConnection()) {
      dbOps.selectDatabase(args.getDatabaseName(), connection);
    }

    List<String> createTableStatements =
        dbOps.createTableSqlStatements(args.createSchemaVersionIndex());

    if (args.getDbType() == DatabaseType.POSTGRES && args.getPostgresMetadataSchema() != null) {
      // search_path is session-scoped: CREATE SCHEMA / SET search_path and CREATE TABLE must share
      // one connection, otherwise unqualified DDL lands in public while CDC grants target the
      // custom schema. Enable auto-commit so DDL is durable before this connection closes (pool
      // default may be EBEAN_DATASOURCE_AUTOCOMMIT=false).
      try (Connection connection = server.dataSource().getConnection()) {
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(true);
        try {
          PostgresSqlSetupSession.ensureSchemaAndSearchPath(
              connection, args.getPostgresMetadataSchema(), args.isCreateSchema());
          for (String sql : createTableStatements) {
            try (Statement st = connection.createStatement()) {
              st.execute(sql);
            }
          }
        } finally {
          connection.setAutoCommit(previousAutoCommit);
        }
      }
    } else {
      for (String sql : createTableStatements) {
        server.sqlUpdate(sql).execute();
      }
    }

    try (Connection connection = server.dataSource().getConnection()) {
      preparePostgresMetadataSession(args, connection);
      dbOps.dropLegacyAspectTableIndexes(connection);
    }
    try (Connection connection = server.dataSource().getConnection()) {
      preparePostgresMetadataSession(args, connection);
      dbOps.ensureAspectIndexes(connection);
    }
    try (Connection connection = server.dataSource().getConnection()) {
      preparePostgresMetadataSession(args, connection);
      dbOps.ensureAspectTableCollation(connection);
    }

    if (args.createSchemaVersionIndex()) {
      try (Connection connection = server.dataSource().getConnection()) {
        preparePostgresMetadataSession(args, connection);
        dbOps.postSetup(connection);
      }
    }
    result.setTablesCreated(1);

    result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
    return result;
  }

  /**
   * Sets {@code search_path} on a new pool connection so unqualified index/table DDL targets {@code
   * postgres.schema}. Does not create the schema (that happens once with the CREATE TABLE
   * connection).
   */
  private static void preparePostgresMetadataSession(SqlSetupArgs args, Connection connection)
      throws SQLException {
    if (args.getDbType() == DatabaseType.POSTGRES && args.getPostgresMetadataSchema() != null) {
      PostgresSqlSetupSession.setSearchPath(connection, args.getPostgresMetadataSchema());
    }
  }

  public boolean containsKey(
      java.util.Map<String, java.util.Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }
}
