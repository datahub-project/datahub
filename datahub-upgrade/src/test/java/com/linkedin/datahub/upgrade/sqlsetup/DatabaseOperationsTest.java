package com.linkedin.datahub.upgrade.sqlsetup;

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
    assertEquals(result, "CREATE USER \"testuser\" WITH LOGIN;");
  }

  @Test
  public void testMysqlCreateIamUserSql() {
    String result = mysqlOps.createIamUserSql("testuser", "testrole");
    assertEquals(
        result,
        "CREATE USER 'testuser'@'%' IDENTIFIED WITH AWSAuthenticationPlugin AS 'testrole';");
  }

  @Test
  public void testPostgresCreateTraditionalUserSql() {
    String result = postgresOps.createTraditionalUserSql("testuser", "testpass");
    assertEquals(result, "CREATE USER \"testuser\" WITH PASSWORD 'testpass';");
  }

  @Test
  public void testMysqlCreateTraditionalUserSql() {
    String result = mysqlOps.createTraditionalUserSql("testuser", "testpass");
    assertEquals(result, "CREATE USER 'testuser'@'%' IDENTIFIED BY 'testpass';");
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
    assertTrue(result.contains("ALTER USER \"cdcuser\" WITH REPLICATION"));
  }

  @Test
  public void testMysqlCreateCdcUserSql() {
    String result = mysqlOps.createCdcUserSql("cdcuser", "cdcpass");
    assertEquals(result, "CREATE USER IF NOT EXISTS 'cdcuser'@'%' IDENTIFIED BY 'cdcpass';");
  }

  @Test
  public void testPostgresGrantCdcPrivilegesSql() {
    String result = postgresOps.grantCdcPrivilegesSql("cdcuser", "testdb");
    assertTrue(result.contains("GRANT CONNECT ON DATABASE \"testdb\" TO \"cdcuser\""));
    assertTrue(result.contains("CREATE PUBLICATION dbz_publication"));
  }

  @Test
  public void testMysqlGrantCdcPrivilegesSql() {
    String result = mysqlOps.grantCdcPrivilegesSql("cdcuser", "testdb");
    assertTrue(result.contains("GRANT SELECT ON `testdb`.* TO 'cdcuser'@'%'"));
    assertTrue(result.contains("GRANT REPLICATION CLIENT ON *.* TO 'cdcuser'@'%'"));
  }

  @Test
  public void testPostgresCreateTableSqlStatements() {
    java.util.List<String> statements = postgresOps.createTableSqlStatements();
    assertNotNull(statements);
    assertTrue(statements.size() >= 2); // At least table creation + one index
    assertTrue(statements.get(0).contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(statements.get(1).contains("CREATE INDEX IF NOT EXISTS timeIndex"));
  }

  @Test
  public void testMysqlCreateTableSqlStatements() {
    java.util.List<String> statements = mysqlOps.createTableSqlStatements();
    assertNotNull(statements);
    assertEquals(statements.size(), 1); // MySQL creates table with indexes in one statement
    assertTrue(statements.get(0).contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(statements.get(0).contains("INDEX timeIndex (createdon)"));
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
