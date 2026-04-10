package com.linkedin.datahub.upgrade.cleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * Drops the DataHub SQL database and any users that were created by the SQL setup job. Uses the
 * admin connection from Ebean (which connects to the server-level database) to issue DDL.
 */
@Slf4j
public class DropDatabaseStep implements UpgradeStep {

  private final Database server;
  private final SqlSetupArgs setupArgs;

  public DropDatabaseStep(Database server, SqlSetupArgs setupArgs) {
    this.server = server;
    this.setupArgs = setupArgs;
  }

  @Override
  public String id() {
    return "DropDatabaseStep";
  }

  @Override
  public int retryCount() {
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      String dbName = setupArgs.getDatabaseName();
      if (dbName == null || dbName.isEmpty()) {
        log.warn("No database name configured — skipping database drop");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }

      try (Connection conn = server.dataSource().getConnection()) {
        conn.setAutoCommit(true);

        dropUsers(conn);
        dropDatabase(conn, dbName);

        log.info("SQL cleanup completed successfully");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("DropDatabaseStep failed: {}", e.getMessage(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void dropDatabase(Connection conn, String dbName) throws Exception {
    validateIdentifier(dbName);
    boolean isPostgres = setupArgs.getDbType() == DatabaseType.POSTGRES;

    if (isPostgres) {
      // Use a parameterized query to avoid SQL injection in the WHERE clause.
      // DROP DATABASE cannot be parameterized, but this SELECT can and should be.
      String terminateSql =
          "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
              + "WHERE datname = ? AND pid <> pg_backend_pid()";
      try (PreparedStatement pstmt = conn.prepareStatement(terminateSql)) {
        pstmt.setString(1, dbName);
        pstmt.execute();
      } catch (Exception e) {
        log.warn("Failed to terminate connections to {}: {}", dbName, e.getMessage());
      }
    }

    // DDL identifiers cannot be parameterized in JDBC; identifier is validated above.
    String dropSql;
    if (isPostgres) {
      dropSql = String.format("DROP DATABASE IF EXISTS \"%s\"", dbName);
    } else {
      dropSql = String.format("DROP DATABASE IF EXISTS `%s`", dbName);
    }

    log.info("Dropping database: {}", dbName);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(dropSql);
    }
    log.info("Successfully dropped database: {}", dbName);
  }

  private void dropUsers(Connection conn) {
    boolean isPostgres = setupArgs.getDbType() == DatabaseType.POSTGRES;

    // Drop application user if it was created
    if (setupArgs.isCreateUser()
        && setupArgs.getCreateUserUsername() != null
        && !setupArgs.getCreateUserUsername().isEmpty()) {
      dropUser(conn, setupArgs.getCreateUserUsername(), isPostgres);
    }

    // Drop CDC user if it was created
    if (setupArgs.isCdcEnabled()
        && setupArgs.getCdcUser() != null
        && !setupArgs.getCdcUser().isEmpty()) {
      dropUser(conn, setupArgs.getCdcUser(), isPostgres);
    }
  }

  private void dropUser(Connection conn, String username, boolean isPostgres) {
    try {
      validateIdentifier(username);
    } catch (IllegalArgumentException e) {
      log.warn("Skipping user drop — invalid identifier '{}': {}", username, e.getMessage());
      return;
    }

    // DDL identifiers cannot be parameterized in JDBC; identifier is validated above.
    String sql;
    if (isPostgres) {
      sql = String.format("DROP USER IF EXISTS \"%s\"", username);
    } else {
      sql = String.format("DROP USER IF EXISTS '%s'@'%%'", username);
    }

    try (Statement stmt = conn.createStatement()) {
      log.info("Dropping user: {}", username);
      stmt.execute(sql);
      log.info("Successfully dropped user: {}", username);
    } catch (Exception e) {
      log.warn("Failed to drop user {}: {}", username, e.getMessage());
    }
  }

  /**
   * Validates that a SQL identifier contains only safe characters. DDL statements (DROP DATABASE,
   * DROP USER) cannot use parameterized placeholders in JDBC, so the identifier must be validated
   * before being interpolated into the statement string.
   */
  private static void validateIdentifier(String name) {
    if (name == null || !name.matches("[a-zA-Z0-9_\\-]+")) {
      throw new IllegalArgumentException("SQL identifier contains unsafe characters: " + name);
    }
  }
}
