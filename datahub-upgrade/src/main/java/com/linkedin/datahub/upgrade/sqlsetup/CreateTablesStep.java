package com.linkedin.datahub.upgrade.sqlsetup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateTablesStep implements UpgradeStep {

  private final Database server;
  private final SqlSetupArgs setupArgs;

  public CreateTablesStep(final Database server, final SqlSetupArgs setupArgs) {
    this.server = server;
    this.setupArgs = setupArgs;
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
    if (args.createDatabase) {
      createDatabaseIfNotExists(args.databaseName, args.dbType);
    }

    // Select the database (MySQL only, PostgreSQL doesn't need this)
    if (DatabaseType.MYSQL.equals(args.dbType)) {
      selectDatabase(args.databaseName);
    }

    // Create metadata_aspect_v2 table
    String createTableSql = getCreateTableSql(args.dbType);
    server.sqlUpdate(createTableSql).execute();
    result.setTablesCreated(1);

    result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
    return result;
  }

  void createDatabaseIfNotExists(String databaseName, DatabaseType dbType) throws SQLException {
    if (DatabaseType.POSTGRES.equals(dbType)) {
      // PostgreSQL database creation with existence check using PreparedStatement
      String checkDbSql = "SELECT 1 FROM pg_database WHERE datname = ?";

      try (Connection connection = server.dataSource().getConnection();
          PreparedStatement stmt = connection.prepareStatement(checkDbSql)) {
        stmt.setString(1, databaseName);
        try (ResultSet result = stmt.executeQuery()) {
          if (!result.next()) {
            // PostgreSQL CREATE DATABASE cannot run inside a transaction block
            // Use direct JDBC connection outside of Ebean transaction
            createPostgresDatabaseDirectly(databaseName);
            log.info("Created PostgreSQL database: {}", databaseName);
          } else {
            log.info("PostgreSQL database {} already exists", databaseName);
          }
        }
      } catch (Exception e) {
        log.warn("PostgreSQL database check failed, assuming database exists: {}", e.getMessage());
        // If we can't check, assume the database exists to avoid unnecessary creation attempts
      }
    } else if (DatabaseType.MYSQL.equals(dbType)) {
      // MySQL database creation with existence check using PreparedStatement
      String checkDbSql =
          "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?";

      try (Connection connection = server.dataSource().getConnection();
          PreparedStatement stmt = connection.prepareStatement(checkDbSql)) {
        stmt.setString(1, databaseName);
        try (ResultSet result = stmt.executeQuery()) {
          if (!result.next()) {
            // Create database using PreparedStatement
            String createDbSql = "CREATE DATABASE `" + databaseName + "`";
            try (PreparedStatement createStmt = connection.prepareStatement(createDbSql)) {
              createStmt.executeUpdate();
              log.info("Created MySQL database: {}", databaseName);
            }
          } else {
            log.info("MySQL database {} already exists", databaseName);
          }
        }
      } catch (Exception e) {
        log.debug("MySQL database check failed, attempting to create: {}", e.getMessage());
        // Fallback: try to create database directly
        try (Connection connection = server.dataSource().getConnection()) {
          String createDbSql = "CREATE DATABASE `" + databaseName + "`";
          try (PreparedStatement createStmt = connection.prepareStatement(createDbSql)) {
            createStmt.executeUpdate();
            log.info("Created MySQL database: {}", databaseName);
          }
        }
      }
    }
  }

  /**
   * Create PostgreSQL database using direct JDBC connection outside of transaction. This is
   * required because PostgreSQL's CREATE DATABASE cannot run inside a transaction block.
   */
  void createPostgresDatabaseDirectly(String databaseName) throws SQLException {
    // Get the data source from Ebean server
    DataSource dataSource = server.dataSource();

    // Create a new connection outside of Ebean's transaction management
    try (Connection connection = dataSource.getConnection()) {
      // Ensure auto-commit is enabled for this connection
      connection.setAutoCommit(true);

      // Check if database exists using PreparedStatement
      String checkDbSql = "SELECT 1 FROM pg_database WHERE datname = ?";
      try (PreparedStatement checkStmt = connection.prepareStatement(checkDbSql)) {
        checkStmt.setString(1, databaseName);
        try (ResultSet rs = checkStmt.executeQuery()) {
          if (!rs.next()) {
            // Database doesn't exist, create it
            // Note: CREATE DATABASE cannot be parameterized, so we use proper identifier quoting
            String escapedDatabaseName = databaseName.replace("\"", "\"\"");
            String createDbSql = "CREATE DATABASE \"" + escapedDatabaseName + "\"";
            try (PreparedStatement createStmt = connection.prepareStatement(createDbSql)) {
              createStmt.executeUpdate();
              log.info("Successfully created PostgreSQL database: {}", databaseName);
            }
          } else {
            log.info("PostgreSQL database {} already exists", databaseName);
          }
        }
      }
    }
  }

  void selectDatabase(String databaseName) throws SQLException {
    // Use PreparedStatement for the USE statement
    try (Connection connection = server.dataSource().getConnection()) {
      String useDbSql = "USE `" + databaseName + "`";
      try (PreparedStatement stmt = connection.prepareStatement(useDbSql)) {
        stmt.executeUpdate();
        log.info("Selected MySQL database: {}", databaseName);
      }
    }
  }

  String getCreateTableSql(DatabaseType dbType) {
    if (DatabaseType.POSTGRES.equals(dbType)) {
      return """
          CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
            urn                           varchar(500) not null,
            aspect                        varchar(200) not null,
            version                       bigint not null,
            metadata                      text not null,
            systemmetadata                text,
            createdon                     timestamp not null,
            createdby                     varchar(255) not null,
            createdfor                    varchar(255),
            CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn, aspect, version)
          );
          CREATE INDEX IF NOT EXISTS timeIndex ON metadata_aspect_v2 (createdon);
          """;
    } else {
      // MySQL
      return """
          CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
            urn                           varchar(500) not null,
            aspect                        varchar(200) not null,
            version                       bigint(20) not null,
            metadata                      longtext not null,
            systemmetadata                longtext,
            createdon                     datetime(6) not null,
            createdby                     varchar(255) not null,
            createdfor                    varchar(255),
            CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn,aspect,version),
            INDEX timeIndex (createdon)
          ) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
          """;
    }
  }

  public boolean containsKey(
      java.util.Map<String, java.util.Optional<String>> parsedArgs, String key) {
    return parsedArgs.containsKey(key)
        && parsedArgs.get(key) != null
        && parsedArgs.get(key).isPresent();
  }
}
