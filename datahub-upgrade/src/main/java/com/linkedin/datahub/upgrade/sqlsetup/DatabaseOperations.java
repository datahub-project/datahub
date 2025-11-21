package com.linkedin.datahub.upgrade.sqlsetup;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for database-specific operations in SqlSetup. This allows for clean separation of
 * database-specific logic and makes it easy to add support for new database types.
 *
 * <p><strong>Important Note on Prepared Statements and DDL:</strong>
 *
 * <p>Many DDL (Data Definition Language) statements in this interface cannot use prepared
 * statements with parameter placeholders (?). This is due to several database-specific limitations:
 *
 * <ul>
 *   <li><strong>Object Names Cannot Be Parameterized:</strong> Database identifiers (table names,
 *       column names, user names, database names) cannot be parameterized in prepared statements.
 *       For example, {@code CREATE TABLE ? (id INT)} is invalid SQL.
 *   <li><strong>PostgreSQL Limitations:</strong> PostgreSQL has strict rules about parameterized
 *       statements. Many DDL operations like {@code CREATE USER}, {@code GRANT}, and {@code ALTER}
 *       statements cannot use parameter placeholders for object names.
 *   <li><strong>MySQL Limitations:</strong> While MySQL is more permissive, many DDL statements
 *       still cannot be parameterized, especially those involving user creation and privilege
 *       management.
 *   <li><strong>SQL Injection Mitigation:</strong> Instead of prepared statements, we use proper
 *       identifier escaping and quoting to prevent SQL injection. For example:
 *       <ul>
 *         <li>PostgreSQL: Double quotes around identifiers {@code "user_name"}
 *         <li>MySQL: Backticks around identifiers {@code `user_name`}
 *         <li>String escaping for passwords and other string literals
 *       </ul>
 * </ul>
 *
 * <p>Where prepared statements <em>are</em> used (like in {@code createDatabaseIfNotExists} and
 * {@code selectDatabase}), they are used for simple operations that can be parameterized, such as
 * checking database existence or executing USE statements.
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
   * Generate SQL statements for granting CDC privileges to a user. Returns a list of individual SQL
   * statements that can be executed separately.
   *
   * @param cdcUser the CDC username
   * @param databaseName the database name
   * @return list of SQL statements for granting CDC privileges
   */
  java.util.List<String> grantCdcPrivilegesSql(String cdcUser, String databaseName);

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
