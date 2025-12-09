package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL-specific implementation of database operations for SqlSetup.
 *
 * <p><strong>PostgreSQL DDL Limitations:</strong>
 *
 * <p>PostgreSQL has strict limitations on prepared statements for DDL operations:
 *
 * <ul>
 *   <li><strong>Object Names Cannot Be Parameterized:</strong> PostgreSQL does not allow parameter
 *       placeholders (?) for database object names in DDL statements. For example:
 *       <ul>
 *         <li>❌ {@code CREATE USER ? WITH PASSWORD ?} - Invalid
 *         <li>✅ {@code CREATE USER "username" WITH PASSWORD 'password'} - Valid
 *       </ul>
 *   <li><strong>Transaction Block Restrictions:</strong> Some DDL operations like {@code CREATE
 *       DATABASE} cannot run inside transaction blocks, requiring special handling with
 *       auto-commit.
 *   <li><strong>Identifier Quoting:</strong> PostgreSQL uses double quotes for identifier quoting
 *       and single quotes for string literals. Proper escaping prevents SQL injection.
 * </ul>
 *
 * <p>This implementation uses string concatenation with proper escaping instead of prepared
 * statements for DDL operations, while still using prepared statements where possible (e.g.,
 * existence checks).
 */
@Slf4j
public class PostgresDatabaseOperations implements DatabaseOperations {

  @Override
  public String createIamUserSql(String username, String iamRole) {
    // PostgreSQL RDS - IAM authentication uses the rds_iam role
    // The iamRole parameter is not used for PostgreSQL (IAM permissions are managed by AWS IAM)
    // The actual IAM permissions are managed by AWS IAM policies, not stored in PostgreSQL
    String escapedUser = escapePostgresIdentifier(username);
    return String.format(
        """
        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = %s) THEN
                CREATE USER %s WITH LOGIN;
            END IF;
        END
        $$;
        GRANT rds_iam TO %s;
        """,
        escapedUser, escapedUser, escapedUser);
  }

  @Override
  public String createTraditionalUserSql(String username, String password) {
    String escapedUser = escapePostgresIdentifier(username);
    String escapedPassword = escapePostgresStringLiteral(password);
    return String.format(
        """
        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = %s) THEN
                CREATE USER %s WITH PASSWORD %s;
            END IF;
        END
        $$;
        """,
        escapedUser, escapedUser, escapedPassword);
  }

  @Override
  public String grantPrivilegesSql(String username, String databaseName) {
    String escapedUser = escapePostgresIdentifier(username);
    String escapedDatabase = escapePostgresIdentifier(databaseName);
    return "GRANT ALL PRIVILEGES ON DATABASE " + escapedDatabase + " TO " + escapedUser + ";";
  }

  @Override
  public String createCdcUserSql(String cdcUser, String cdcPassword) {
    // PostgreSQL CDC user creation with comprehensive privileges
    // Properly escape identifiers and string literals to prevent SQL injection
    String escapedUser = escapePostgresIdentifier(cdcUser);
    String escapedPassword = escapePostgresStringLiteral(cdcPassword);
    String escapedUserLiteral = escapePostgresStringLiteral(cdcUser);

    return String.format(
        """
        DO
        $$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = %s) THEN
                CREATE USER %s WITH PASSWORD %s;
            END IF;
        END
        $$;
        ALTER USER %s WITH REPLICATION;
        """,
        escapedUserLiteral, escapedUser, escapedPassword, escapedUser);
  }

  @Override
  public java.util.List<String> grantCdcPrivilegesSql(String cdcUser, String databaseName) {
    // PostgreSQL comprehensive CDC privileges (matching original init-cdc.sql)
    // Return as separate statements since JDBC doesn't support multiple statements in one execution
    String escapedUser = escapePostgresIdentifier(cdcUser);
    String escapedDatabase = escapePostgresIdentifier(databaseName);

    return java.util.Arrays.asList(
        String.format("GRANT CONNECT ON DATABASE %s TO %s", escapedDatabase, escapedUser),
        String.format("GRANT USAGE ON SCHEMA public TO %s", escapedUser),
        String.format("GRANT CREATE ON DATABASE %s TO %s", escapedDatabase, escapedUser),
        String.format("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", escapedUser),
        String.format(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO %s", escapedUser),
        String.format("ALTER USER %s WITH SUPERUSER", escapedUser),
        String.format("ALTER TABLE public.metadata_aspect_v2 OWNER TO %s", escapedUser),
        "ALTER TABLE public.metadata_aspect_v2 REPLICA IDENTITY FULL",
        "CREATE PUBLICATION dbz_publication FOR TABLE public.metadata_aspect_v2");
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
   *
   * <p><strong>Why Prepared Statements Cannot Be Used:</strong>
   *
   * <p>PostgreSQL's {@code CREATE DATABASE} statement cannot be parameterized with prepared
   * statements. The database name must be embedded directly in the SQL string. This is a
   * fundamental limitation of PostgreSQL's DDL implementation, not a choice in this code.
   *
   * <p>Example of what doesn't work:
   *
   * <pre>{@code
   * // ❌ This is invalid PostgreSQL syntax
   * PreparedStatement stmt = connection.prepareStatement("CREATE DATABASE ?");
   * stmt.setString(1, databaseName);
   * }</pre>
   *
   * <p>Instead, we use proper identifier escaping to prevent SQL injection:
   *
   * <pre>{@code
   * // ✅ This is the correct approach
   * String escapedDatabaseName = databaseName.replace("\"", "\"\"");
   * String createDbSql = "CREATE DATABASE \"" + escapedDatabaseName + "\"";
   * }</pre>
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

  /**
   * Escape PostgreSQL identifier by wrapping in double quotes and escaping any existing double
   * quotes. This prevents SQL injection when using identifiers in DDL statements.
   *
   * @param identifier the identifier to escape
   * @return the escaped identifier wrapped in double quotes
   */
  private String escapePostgresIdentifier(String identifier) {
    if (identifier == null) {
      throw new IllegalArgumentException("Identifier cannot be null");
    }
    // Escape double quotes by doubling them, then wrap in double quotes
    String escaped = identifier.replace("\"", "\"\"");
    return "\"" + escaped + "\"";
  }

  /**
   * Escape PostgreSQL string literal by wrapping in single quotes and escaping any existing single
   * quotes. This prevents SQL injection when using string literals in DDL statements.
   *
   * @param literal the string literal to escape
   * @return the escaped string literal wrapped in single quotes
   */
  private String escapePostgresStringLiteral(String literal) {
    if (literal == null) {
      throw new IllegalArgumentException("String literal cannot be null");
    }
    // Escape single quotes by doubling them, then wrap in single quotes
    String escaped = literal.replace("'", "''");
    return "'" + escaped + "'";
  }
}
