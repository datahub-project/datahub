package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for database-specific operations in SqlSetup. This allows for clean separation of
 * database-specific logic and makes it easy to add support for new database types.
 */
public interface DatabaseOperations {

  /**
   * Create a DatabaseOperations implementation for the specified database type.
   *
   * @param dbType the database type
   * @return the appropriate DatabaseOperations implementation
   * @throws IllegalArgumentException if the database type is not supported
   */
  static DatabaseOperations create(DatabaseType dbType) {
    return switch (dbType) {
      case POSTGRES -> new PostgresDatabaseOperations();
      case MYSQL -> new MySqlDatabaseOperations();
    };
  }

  /**
   * Generate SQL for creating an IAM-authenticated user.
   *
   * @param username the username to create
   * @param iamRole the IAM role for authentication
   * @return the SQL statement for creating the IAM user
   */
  String createIamUserSql(String username, String iamRole);

  /**
   * Generate SQL for creating a traditional password-authenticated user.
   *
   * @param username the username to create
   * @param password the password for the user
   * @return the SQL statement for creating the traditional user
   */
  String createTraditionalUserSql(String username, String password);

  /**
   * Generate SQL for granting privileges to a user on a database.
   *
   * @param username the username to grant privileges to
   * @param databaseName the database name
   * @return the SQL statement for granting privileges
   */
  String grantPrivilegesSql(String username, String databaseName);

  /**
   * Generate SQL for creating a CDC user.
   *
   * @param cdcUser the CDC username
   * @param cdcPassword the CDC user password
   * @return the SQL statement for creating the CDC user
   */
  String createCdcUserSql(String cdcUser, String cdcPassword);

  /**
   * Generate SQL for granting CDC privileges to a user.
   *
   * @param cdcUser the CDC username
   * @param databaseName the database name
   * @return the SQL statement for granting CDC privileges
   */
  String grantCdcPrivilegesSql(String cdcUser, String databaseName);

  /**
   * Generate SQL statements for creating the metadata_aspect_v2 table and its indexes. Returns a
   * list of individual SQL statements that can be executed separately.
   *
   * @return list of SQL statements to create the table and indexes
   */
  java.util.List<String> createTableSqlStatements();

  /**
   * Create a database if it doesn't exist.
   *
   * @param databaseName the name of the database to create
   * @param connection the database connection to use
   * @throws SQLException if database creation fails
   */
  void createDatabaseIfNotExists(String databaseName, Connection connection) throws SQLException;

  /**
   * Select/use a specific database (MySQL only, PostgreSQL doesn't need this).
   *
   * @param databaseName the name of the database to select
   * @param connection the database connection to use
   * @throws SQLException if database selection fails
   */
  void selectDatabase(String databaseName, Connection connection) throws SQLException;

  /**
   * Modify JDBC URL for database creation purposes.
   *
   * @param originalUrl the original JDBC URL
   * @param createDb whether database creation is needed
   * @return the modified JDBC URL
   */
  String modifyJdbcUrl(String originalUrl, boolean createDb);
}
