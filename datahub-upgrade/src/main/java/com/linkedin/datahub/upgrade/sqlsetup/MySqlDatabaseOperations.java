package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/**
 * MySQL-specific implementation of database operations for SqlSetup.
 *
 * <p><strong>MySQL DDL Limitations:</strong>
 *
 * <p>While MySQL is more permissive than PostgreSQL, it still has limitations on prepared
 * statements for DDL operations:
 *
 * <ul>
 *   <li><strong>Object Names Cannot Be Parameterized:</strong> MySQL does not allow parameter
 *       placeholders (?) for database object names in most DDL statements. For example:
 *       <ul>
 *         <li>❌ {@code CREATE USER ?@'%' IDENTIFIED BY ?} - Invalid
 *         <li>✅ {@code CREATE USER 'username'@'%' IDENTIFIED BY 'password'} - Valid
 *       </ul>
 *   <li><strong>Identifier Quoting:</strong> MySQL uses backticks (`) for identifier quoting and
 *       single quotes for string literals. Proper escaping prevents SQL injection.
 *   <li><strong>Limited Prepared Statement Support:</strong> Some DDL operations like {@code CREATE
 *       USER} and {@code GRANT} statements cannot be parameterized, requiring string concatenation.
 * </ul>
 *
 * <p>This implementation uses string concatenation with proper escaping for DDL operations that
 * cannot be parameterized, while using prepared statements where possible (e.g., existence checks).
 */
@Slf4j
public class MySqlDatabaseOperations implements DatabaseOperations {

  @Override
  public String createIamUserSql(String username, String iamRole) {
    // The iamRole parameter is not used for MySQL (unlike PostgreSQL)
    // The actual IAM permissions are managed by AWS IAM policies, not stored in MySQL
    String escapedUser = escapeMysqlStringLiteral(username);
    return "CREATE USER IF NOT EXISTS "
        + escapedUser
        + "@'%' IDENTIFIED WITH AWSAuthenticationPlugin AS 'RDS';";
  }

  @Override
  public String createTraditionalUserSql(String username, String password) {
    // MySQL - traditional authentication
    String escapedUser = escapeMysqlStringLiteral(username);
    String escapedPassword = escapeMysqlStringLiteral(password);
    return "CREATE USER IF NOT EXISTS "
        + escapedUser
        + "@'%' IDENTIFIED BY "
        + escapedPassword
        + ";";
  }

  @Override
  public String grantPrivilegesSql(String username, String databaseName) {
    // MySQL - properly escape identifiers
    String escapedUser = escapeMysqlStringLiteral(username);
    String escapedDatabase = escapeMysqlIdentifier(databaseName);
    return "GRANT ALL PRIVILEGES ON " + escapedDatabase + ".* TO " + escapedUser + "@'%';";
  }

  @Override
  public String createCdcUserSql(String cdcUser, String cdcPassword) {
    // MySQL - CDC user with replication privileges (matching original init-cdc.sql)
    // Properly escape string literals to prevent SQL injection
    String escapedUser = escapeMysqlStringLiteral(cdcUser);
    String escapedPassword = escapeMysqlStringLiteral(cdcPassword);

    return "CREATE USER IF NOT EXISTS "
        + escapedUser
        + "@'%' IDENTIFIED BY "
        + escapedPassword
        + ";";
  }

  @Override
  public java.util.List<String> grantCdcPrivilegesSql(String cdcUser, String databaseName) {
    // MySQL - comprehensive CDC privileges (matching original init-cdc.sql)
    // Return as separate statements since JDBC doesn't support multiple statements in one execution
    String escapedUser = escapeMysqlStringLiteral(cdcUser);
    String escapedDatabase = escapeMysqlIdentifier(databaseName);

    return java.util.Arrays.asList(
        String.format("GRANT SELECT ON %s.* TO %s@'%%'", escapedDatabase, escapedUser),
        String.format("GRANT RELOAD ON *.* TO %s@'%%'", escapedUser),
        String.format("GRANT REPLICATION CLIENT ON *.* TO %s@'%%'", escapedUser),
        String.format("GRANT REPLICATION SLAVE ON *.* TO %s@'%%'", escapedUser),
        "FLUSH PRIVILEGES");
  }

  @Override
  public java.util.List<String> createTableSqlStatements() {
    return java.util.Arrays.asList(
        """
        CREATE TABLE IF NOT EXISTS metadata_aspect_v2 (
          urn                           varchar(500) not null,
          aspect                        varchar(200) not null,
          version                       bigint not null,
          metadata                      longtext not null,
          systemmetadata                longtext,
          createdon                     timestamp(6) not null,
          createdby                     varchar(255) not null,
          createdfor                    varchar(255),
          CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY (urn, aspect, version),
          INDEX timeIndex (createdon),
          INDEX urnIndex (urn),
          INDEX aspectIndex (aspect),
          INDEX versionIndex (version)
        );
        """);
  }

  @Override
  public void createDatabaseIfNotExists(String databaseName, Connection connection)
      throws SQLException {
    // MySQL database creation with existence check using PreparedStatement
    String checkDbSql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?";

    try (PreparedStatement stmt = connection.prepareStatement(checkDbSql)) {
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
      try (PreparedStatement createStmt =
          connection.prepareStatement("CREATE DATABASE `" + databaseName + "`")) {
        createStmt.executeUpdate();
        log.info("Created MySQL database: {}", databaseName);
      }
    }
  }

  @Override
  public void selectDatabase(String databaseName, Connection connection) throws SQLException {
    // Use PreparedStatement for the USE statement
    String useDbSql = "USE `" + databaseName + "`";
    try (PreparedStatement stmt = connection.prepareStatement(useDbSql)) {
      stmt.executeUpdate();
      log.info("Selected MySQL database: {}", databaseName);
    }
  }

  @Override
  public String modifyJdbcUrl(String originalUrl, boolean createDb) {
    if (createDb) {
      // For MySQL: remove database name to connect to server only (needed for database creation)
      String modifiedUrl = JdbcUrlParser.createUrlWithoutDatabase(originalUrl);
      log.info(
          "SqlSetup MySQL: Modifying database URL from '{}' to '{}' for database creation",
          originalUrl,
          modifiedUrl);
      return modifiedUrl;
    } else {
      // For MySQL: use original URL if no database creation needed
      log.info(
          "SqlSetup MySQL: Using original database URL '{}' (no database creation needed)",
          originalUrl);
      return originalUrl;
    }
  }

  /**
   * Escape MySQL identifier by wrapping in backticks and escaping any existing backticks. This
   * prevents SQL injection when using identifiers in DDL statements.
   *
   * @param identifier the identifier to escape
   * @return the escaped identifier wrapped in backticks
   */
  private String escapeMysqlIdentifier(String identifier) {
    if (identifier == null) {
      throw new IllegalArgumentException("Identifier cannot be null");
    }
    // Escape backticks by doubling them, then wrap in backticks
    String escaped = identifier.replace("`", "``");
    return "`" + escaped + "`";
  }

  /**
   * Escape MySQL string literal by wrapping in single quotes and escaping any existing single
   * quotes. This prevents SQL injection when using string literals in DDL statements.
   *
   * @param literal the string literal to escape
   * @return the escaped string literal wrapped in single quotes
   */
  private String escapeMysqlStringLiteral(String literal) {
    if (literal == null) {
      throw new IllegalArgumentException("String literal cannot be null");
    }
    // Escape single quotes by doubling them, then wrap in single quotes
    String escaped = literal.replace("'", "''");
    return "'" + escaped + "'";
  }
}
