package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;

/** MySQL-specific implementation of database operations for SqlSetup. */
@Slf4j
public class MySqlDatabaseOperations implements DatabaseOperations {

  @Override
  public String createIamUserSql(String username, String iamRole) {
    // MySQL - IAM authentication with configurable role
    return "CREATE USER '"
        + username
        + "'@'%' IDENTIFIED WITH AWSAuthenticationPlugin AS '"
        + iamRole
        + "';";
  }

  @Override
  public String createTraditionalUserSql(String username, String password) {
    // MySQL - traditional authentication
    return "CREATE USER '" + username + "'@'%' IDENTIFIED BY '" + password + "';";
  }

  @Override
  public String grantPrivilegesSql(String username, String databaseName) {
    // MySQL
    return "GRANT ALL PRIVILEGES ON `" + databaseName + "`.* TO '" + username + "'@'%';";
  }

  @Override
  public String createCdcUserSql(String cdcUser, String cdcPassword) {
    // MySQL - CDC user with replication privileges (matching original init-cdc.sql)
    return "CREATE USER IF NOT EXISTS '" + cdcUser + "'@'%' IDENTIFIED BY '" + cdcPassword + "';";
  }

  @Override
  public String grantCdcPrivilegesSql(String cdcUser, String databaseName) {
    // MySQL - comprehensive CDC privileges (matching original init-cdc.sql)
    return String.format(
        """
        GRANT SELECT ON `%s`.* TO '%s'@'%%';
        GRANT RELOAD ON *.* TO '%s'@'%%';
        GRANT REPLICATION CLIENT ON *.* TO '%s'@'%%';
        GRANT REPLICATION SLAVE ON *.* TO '%s'@'%%';
        FLUSH PRIVILEGES;
        """,
        databaseName, cdcUser, cdcUser, cdcUser, cdcUser);
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
}
