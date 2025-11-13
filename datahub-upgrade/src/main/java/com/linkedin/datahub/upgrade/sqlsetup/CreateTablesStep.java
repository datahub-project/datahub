package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.SQLException;
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

    // Create metadata_aspect_v2 table and indexes
    java.util.List<String> createTableStatements = dbOps.createTableSqlStatements();
    for (String sql : createTableStatements) {
      server.sqlUpdate(sql).execute();
    }
    result.setTablesCreated(1);

    result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
    return result;
  }

  public boolean containsKey(
      java.util.Map<String, java.util.Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }
}
