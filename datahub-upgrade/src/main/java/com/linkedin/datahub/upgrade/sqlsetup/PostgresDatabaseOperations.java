package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** PostgreSQL-specific implementation of database operations for SqlSetup. */
@Slf4j
public class PostgresDatabaseOperations implements DatabaseOperations {

  @Override
  public String createIamUserSql(String username, String iamRole) {
    // PostgreSQL - IAM authentication (requires additional setup)
    return "CREATE USER \"" + username + "\" WITH LOGIN;";
  }

  @Override
  public String createTraditionalUserSql(String username, String password) {
    return "CREATE USER \"" + username + "\" WITH PASSWORD '" + password + "';";
  }

  @Override
  public String grantPrivilegesSql(String username, String databaseName) {
    return "GRANT ALL PRIVILEGES ON DATABASE \"" + databaseName + "\" TO \"" + username + "\";";
  }

  @Override
  public String createCdcUserSql(String cdcUser, String cdcPassword) {
    // PostgreSQL CDC user creation with comprehensive privileges
    return String.format(
        """
        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN
                CREATE USER "%s" WITH PASSWORD '%s';
            END IF;
        END
        $$;
        ALTER USER "%s" WITH REPLICATION;
        """,
        cdcUser, cdcUser, cdcPassword, cdcUser);
  }

  @Override
  public String grantCdcPrivilegesSql(String cdcUser, String databaseName) {
    // PostgreSQL comprehensive CDC privileges (matching original init-cdc.sql)
    return String.format(
        """
        GRANT CONNECT ON DATABASE "%s" TO "%s";
        GRANT USAGE ON SCHEMA public TO "%s";
        GRANT CREATE ON DATABASE "%s" TO "%s";
        GRANT SELECT ON ALL TABLES IN SCHEMA public TO "%s";
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO "%s";
        ALTER USER "%s" WITH SUPERUSER;
        ALTER TABLE public.metadata_aspect_v2 OWNER TO "%s";
        ALTER TABLE public.metadata_aspect_v2 REPLICA IDENTITY FULL;
        CREATE PUBLICATION dbz_publication FOR TABLE public.metadata_aspect_v2;
        """,
        databaseName, cdcUser, cdcUser, databaseName, cdcUser, cdcUser, cdcUser, cdcUser, cdcUser);
  }

  @Override
  public java.util.List<String> createTableSqlStatements() {
    return java.util.Arrays.asList(
        """
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
        """,
        "CREATE INDEX IF NOT EXISTS timeIndex ON metadata_aspect_v2 (createdon);",
        "CREATE INDEX IF NOT EXISTS urnIndex ON metadata_aspect_v2 (urn);",
        "CREATE INDEX IF NOT EXISTS aspectIndex ON metadata_aspect_v2 (aspect);",
        "CREATE INDEX IF NOT EXISTS versionIndex ON metadata_aspect_v2 (version);");
  }

  @Override
  public void createDatabaseIfNotExists(String databaseName, Connection connection)
      throws SQLException {
    // PostgreSQL database creation with existence check using PreparedStatement
    String checkDbSql = "SELECT 1 FROM pg_database WHERE datname = ?";

    try (PreparedStatement stmt = connection.prepareStatement(checkDbSql)) {
      stmt.setString(1, databaseName);
      try (ResultSet result = stmt.executeQuery()) {
        if (!result.next()) {
          // PostgreSQL CREATE DATABASE cannot run inside a transaction block
          // Use direct JDBC connection outside of Ebean transaction
          createPostgresDatabaseDirectly(databaseName, connection);
          log.info("Created PostgreSQL database: {}", databaseName);
        } else {
          log.info("PostgreSQL database {} already exists", databaseName);
        }
      }
    } catch (Exception e) {
      log.warn("PostgreSQL database check failed, assuming database exists: {}", e.getMessage());
      // If we can't check, assume the database exists to avoid unnecessary creation attempts
    }
  }

  @Override
  public void selectDatabase(String databaseName, Connection connection) throws SQLException {
    // PostgreSQL doesn't need a USE statement - it connects directly to the target database
    log.debug("PostgreSQL doesn't require database selection, skipping");
  }

  @Override
  public String modifyJdbcUrl(String originalUrl, boolean createDb) {
    // For PostgreSQL: always use the original URL (target database)
    // Database creation will be handled separately using a direct JDBC connection
    log.info(
        "SqlSetup PostgreSQL: Using original database URL '{}' (database creation handled separately)",
        originalUrl);
    return originalUrl;
  }

  /**
   * Create PostgreSQL database using direct JDBC connection outside of transaction. This is
   * required because PostgreSQL's CREATE DATABASE cannot run inside a transaction block.
   */
  private void createPostgresDatabaseDirectly(String databaseName, Connection connection)
      throws SQLException {
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
