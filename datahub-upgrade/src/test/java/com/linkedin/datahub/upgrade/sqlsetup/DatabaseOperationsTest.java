package com.linkedin.datahub.upgrade.sqlsetup;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for DatabaseOperations implementations. */
public class DatabaseOperationsTest {

  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;

  private PostgresDatabaseOperations postgresOps;
  private MySqlDatabaseOperations mysqlOps;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    postgresOps = new PostgresDatabaseOperations();
    mysqlOps = new MySqlDatabaseOperations();
  }

  @Test
  public void testPostgresCreateIamUserSql() {
    String result = postgresOps.createIamUserSql("testuser", "testrole");
    assertTrue(result.contains("IF NOT EXISTS"));
    assertTrue(result.contains("CREATE USER"));
    assertTrue(result.contains("\"testuser\""));
    assertTrue(result.contains("WITH LOGIN"));
  }

  @Test
  public void testMysqlCreateIamUserSql() {
    String result = mysqlOps.createIamUserSql("testuser", null);
    assertTrue(result.contains("CREATE USER IF NOT EXISTS"));
    assertTrue(result.contains("'testuser'@'%'"));
    assertTrue(result.contains("IDENTIFIED WITH AWSAuthenticationPlugin"));
    assertTrue(result.contains("AS 'RDS'")); // MySQL uses 'RDS' constant, not the role parameter
  }

  @Test
  public void testPostgresCreateTraditionalUserSql() {
    String result = postgresOps.createTraditionalUserSql("testuser", "testpass");
    assertTrue(result.contains("IF NOT EXISTS"));
    assertTrue(result.contains("CREATE USER"));
    assertTrue(result.contains("\"testuser\""));
    assertTrue(result.contains("WITH PASSWORD"));
    assertTrue(result.contains("testpass"));
  }

  @Test
  public void testMysqlCreateTraditionalUserSql() {
    String result = mysqlOps.createTraditionalUserSql("testuser", "testpass");
    assertTrue(result.contains("CREATE USER IF NOT EXISTS"));
    assertTrue(result.contains("'testuser'@'%'"));
    assertTrue(result.contains("IDENTIFIED BY"));
    assertTrue(result.contains("testpass"));
  }

  @Test
  public void testPostgresGrantPrivilegesSql() {
    String result = postgresOps.grantPrivilegesSql("testuser", "testdb");
    assertEquals(result, "GRANT ALL PRIVILEGES ON DATABASE \"testdb\" TO \"testuser\";");
  }

  @Test
  public void testMysqlGrantPrivilegesSql() {
    String result = mysqlOps.grantPrivilegesSql("testuser", "testdb");
    assertEquals(result, "GRANT ALL PRIVILEGES ON `testdb`.* TO 'testuser'@'%';");
  }

  @Test
  public void testPostgresCreateCdcUserSql() {
    String result = postgresOps.createCdcUserSql("cdcuser", "cdcpass");
    assertTrue(result.contains("CREATE USER \"cdcuser\""));
    // ALTER USER WITH REPLICATION moved to grantCdcPrivilegesSql()
    assertTrue(!result.contains("ALTER USER"));
    // Verify the IF NOT EXISTS comparison is against the username (not password)
    assertTrue(result.contains("rolname = 'cdcuser'"));
  }

  @Test
  public void testPostgresCreateCdcUserSqlWithSpecialCharacters() {
    // Test with username containing quotes and password containing single quotes
    String result =
        postgresOps.createCdcUserSql("user\"with\"quotes", "password'with'single'quotes");
    // Verify username is properly escaped (quotes should be doubled)
    assertTrue(result.contains("\"user\"\"with\"\"quotes\""));
    // Verify password is properly escaped (single quotes should be doubled)
    assertTrue(result.contains("'password''with''single''quotes'"));
    // Verify the comparison uses the correct variable (username, not password)
    assertTrue(
        result.contains("rolname = 'user\"with\"quotes'")
            || result.contains("rolname = 'user\"\"with\"\"quotes'"));
  }

  @Test
  public void testMysqlCreateCdcUserSql() {
    String result = mysqlOps.createCdcUserSql("cdcuser", "cdcpass");
    assertEquals(result, "CREATE USER IF NOT EXISTS 'cdcuser'@'%' IDENTIFIED BY 'cdcpass';");
  }

  @Test
  public void testPostgresGrantCdcPrivilegesSql() {
    java.util.List<String> statements = postgresOps.grantCdcPrivilegesSql("cdcuser", "testdb");
    assertNotNull(statements);
    assertTrue(statements.size() > 0);
    String allStatements = String.join(" ", statements);
    // Verify ALTER USER WITH REPLICATION is now in grants (moved from createCdcUserSql)
    assertTrue(allStatements.contains("ALTER USER \"cdcuser\" WITH REPLICATION"));
    assertTrue(allStatements.contains("GRANT CONNECT ON DATABASE \"testdb\" TO \"cdcuser\""));
    assertTrue(allStatements.contains("CREATE PUBLICATION dbz_publication"));
  }

  @Test
  public void testMysqlGrantCdcPrivilegesSql() {
    java.util.List<String> statements = mysqlOps.grantCdcPrivilegesSql("cdcuser", "testdb");
    assertNotNull(statements);
    assertTrue(statements.size() > 0);
    String allStatements = String.join(" ", statements);
    assertTrue(allStatements.contains("GRANT SELECT ON `testdb`.* TO 'cdcuser'@'%'"));
    assertTrue(allStatements.contains("GRANT REPLICATION CLIENT ON *.* TO 'cdcuser'@'%'"));
  }

  @Test
  public void testPostgresCreateTableSqlStatements() {
    java.util.List<String> statements = postgresOps.createTableSqlStatements(false);
    assertNotNull(statements);
    assertTrue(statements.size() >= 2); // At least table creation + one index
    assertTrue(statements.get(0).contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(statements.get(1).contains("CREATE INDEX IF NOT EXISTS timeIndex"));
    assertTrue(statements.stream().noneMatch(s -> s.contains("schemaVersionIndex")));
  }

  @Test
  public void testPostgresCreateTableSqlStatementsWithSchemaVersionIndex() {
    // schemaVersionIndex is created via postSetup, not in the statement list
    java.util.List<String> statements = postgresOps.createTableSqlStatements(true);
    assertTrue(statements.stream().noneMatch(s -> s.contains("schemaVersionIndex")));
  }

  @Test
  public void testPostgresPostSetupDropsInvalidIndexThenCreates() throws SQLException {
    // Simulate pg_index returning a row (invalid index exists)
    when(mockConnection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockConnection.createStatement()).thenReturn(mockPreparedStatement);

    postgresOps.postSetup(mockConnection);

    verify(mockPreparedStatement).execute("DROP INDEX CONCURRENTLY schemaversionindex");
    verify(mockPreparedStatement)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS schemaVersionIndex ON metadata_aspect_v2 ((systemmetadata::jsonb ->> 'schemaVersion'));");
  }

  @Test
  public void testPostgresPostSetupCreatesIndexWhenNoneInvalid() throws SQLException {
    // Simulate pg_index returning no rows (no invalid index)
    when(mockConnection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
        .thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);
    when(mockConnection.createStatement()).thenReturn(mockPreparedStatement);

    postgresOps.postSetup(mockConnection);

    verify(mockPreparedStatement, never()).execute("DROP INDEX CONCURRENTLY schemaVersionIndex");
    verify(mockPreparedStatement)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS schemaVersionIndex ON metadata_aspect_v2 ((systemmetadata::jsonb ->> 'schemaVersion'));");
  }

  @Test
  public void testMysqlCreateTableSqlStatements() {
    java.util.List<String> statements = mysqlOps.createTableSqlStatements(false);
    assertNotNull(statements);
    assertEquals(statements.size(), 1); // MySQL creates table with indexes in one statement
    assertTrue(statements.get(0).contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(statements.get(0).contains("INDEX timeIndex (createdon)"));
  }

  @Test
  public void testMysqlCreateTableSqlStatementsMatchesMysqlSetupCharsetAndCollation() {
    java.util.List<String> statements = mysqlOps.createTableSqlStatements(false);
    assertNotNull(statements);
    String ddl = statements.get(0);
    assertTrue(
        ddl.contains("CHARACTER SET utf8mb4"),
        "Table DDL should match docker/mysql-setup/init.sql charset to avoid case/encoding regression");
    assertTrue(
        ddl.contains("COLLATE utf8mb4_bin"),
        "Table DDL should match docker/mysql-setup/init.sql collation (case-sensitive)");
  }

  @Test
  public void testPostgresModifyJdbcUrl() {
    String originalUrl = "jdbc:postgresql://localhost:5432/testdb";
    String result = postgresOps.modifyJdbcUrl(originalUrl, true);
    assertEquals(result, originalUrl); // PostgreSQL always uses original URL
  }

  @Test
  public void testMysqlModifyJdbcUrl() {
    String originalUrl = "jdbc:mysql://localhost:3306/testdb";
    String result = mysqlOps.modifyJdbcUrl(originalUrl, true);
    assertTrue(result.contains("jdbc:mysql://localhost:3306")); // Should remove database name
    assertTrue(!result.contains("/testdb")); // Database name should be removed
  }

  @Test
  public void testDatabaseOperationsFactory() {
    DatabaseOperations postgresOps = DatabaseOperations.create(DatabaseType.POSTGRES);
    assertNotNull(postgresOps);
    assertTrue(postgresOps instanceof PostgresDatabaseOperations);

    DatabaseOperations mysqlOps = DatabaseOperations.create(DatabaseType.MYSQL);
    assertNotNull(mysqlOps);
    assertTrue(mysqlOps instanceof MySqlDatabaseOperations);
  }
}
