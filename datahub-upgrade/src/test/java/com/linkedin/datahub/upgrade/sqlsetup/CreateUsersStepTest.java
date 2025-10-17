package com.linkedin.datahub.upgrade.sqlsetup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import io.ebean.SqlUpdate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
import javax.sql.DataSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateUsersStepTest {

  @Mock private Database mockDatabase;
  @Mock private SqlUpdate mockSqlUpdate;
  @Mock private SqlSetupArgs mockSetupArgs;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;
  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;

  private CreateUsersStep createUsersStep;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    createUsersStep = new CreateUsersStep(mockDatabase, mockSetupArgs);
    when(mockUpgradeContext.report()).thenReturn(mockUpgradeReport);

    // Setup mock DataSource chain for PreparedStatement approach
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false); // Default: database doesn't exist

    // Setup mock to return SqlUpdate mock when sqlUpdate is called (for table creation)
    when(mockDatabase.sqlUpdate(anyString())).thenReturn(mockSqlUpdate);
    when(mockSqlUpdate.execute()).thenReturn(1); // Return 1 for successful execution
  }

  @Test
  public void testId() {
    assertEquals(createUsersStep.id(), "CreateUsersStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(createUsersStep.retryCount(), 0);
  }

  @Test
  public void testExecutableSuccess() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = false;
    mockSetupArgs.createUserUsername = "testuser";
    mockSetupArgs.createUserPassword = "testpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine(contains("Users created:"));
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableWithCreateUsersDisabled() throws SQLException {
    mockSetupArgs.createUser = false;

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine("Users created: 0");
  }

  @Test
  public void testExecutableWithIamAuth() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = true;
    mockSetupArgs.createUserUsername = "iamuser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine("Users created: 1");
  }

  @Test
  public void testExecutableWithPostgresTraditional() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = false;
    mockSetupArgs.createUserUsername = "pguser";
    mockSetupArgs.createUserPassword = "pgpass";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine("Users created: 1");
  }

  @Test
  public void testExecutableWithPostgresIam() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = true;
    mockSetupArgs.createUserUsername = "pgiamuser";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine("Users created: 1");
  }

  @Test
  public void testExecutableWithException() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = false;
    mockSetupArgs.createUserUsername = "testuser";
    mockSetupArgs.createUserPassword = "testpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("Database connection failed"));

    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateUsersStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    verify(mockUpgradeReport).addLine("Creating database users...");
    verify(mockUpgradeReport).addLine(contains("Error during execution:"));
  }

  @Test
  public void testContainsKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));
    testMap.put("key2", java.util.Optional.of("value2"));
    testMap.put("key3", java.util.Optional.empty());

    // Test with existing key that has a value
    boolean result1 = createUsersStep.containsKey(testMap, "key1");
    assertTrue(result1);

    // Test with existing key that has empty optional
    boolean result2 = createUsersStep.containsKey(testMap, "key3");
    assertTrue(!result2);

    // Test with non-existing key
    boolean result3 = createUsersStep.containsKey(testMap, "key4");
    assertTrue(!result3);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testContainsKeyWithNullMap() {
    createUsersStep.containsKey(null, "key1");
  }

  @Test
  public void testContainsKeyWithNullKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));

    boolean result = createUsersStep.containsKey(testMap, null);
    assertTrue(!result);
  }

  @Test
  public void testCreateIamUser() throws SQLException {
    mockSetupArgs.createUserUsername = "iamuser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = new SqlSetupResult();
    createUsersStep.createIamUser(mockSetupArgs, result);

    assertEquals(result.getUsersCreated(), 1);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testCreateTraditionalUser() throws SQLException {
    mockSetupArgs.createUserUsername = "traduser";
    mockSetupArgs.createUserPassword = "tradpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = new SqlSetupResult();
    createUsersStep.createTraditionalUser(mockSetupArgs, result);

    assertEquals(result.getUsersCreated(), 1);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testGetCreateIamUserSqlPostgres() throws Exception {
    mockSetupArgs.createUserUsername = "pgiamuser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;

    String result =
        createUsersStep.getCreateIamUserSql(
            DatabaseType.POSTGRES, "pgiamuser", "arn:aws:iam::123456789012:role/datahub-role");

    assertNotNull(result);
    assertTrue(result.contains("CREATE USER \"pgiamuser\" WITH LOGIN"));
  }

  @Test
  public void testGetCreateIamUserSqlMysql() throws Exception {
    mockSetupArgs.createUserUsername = "mysqluser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.MYSQL;

    String result =
        createUsersStep.getCreateIamUserSql(
            DatabaseType.MYSQL, "mysqluser", "arn:aws:iam::123456789012:role/datahub-role");

    assertNotNull(result);
    assertTrue(
        result.contains("CREATE USER 'mysqluser'@'%' IDENTIFIED WITH AWSAuthenticationPlugin"));
    assertTrue(result.contains("AS 'arn:aws:iam::123456789012:role/datahub-role'"));
  }

  @Test
  public void testGetCreateTraditionalUserSqlPostgres() throws Exception {
    mockSetupArgs.createUserUsername = "pguser";
    mockSetupArgs.createUserPassword = "pgpass";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;

    String result =
        createUsersStep.getCreateTraditionalUserSql(DatabaseType.POSTGRES, "pguser", "pgpass");

    assertNotNull(result);
    assertTrue(result.contains("CREATE USER \"pguser\" WITH PASSWORD 'pgpass'"));
  }

  @Test
  public void testGetCreateTraditionalUserSqlMysql() throws Exception {
    mockSetupArgs.createUserUsername = "mysqluser";
    mockSetupArgs.createUserPassword = "mysqlpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;

    String result =
        createUsersStep.getCreateTraditionalUserSql(DatabaseType.MYSQL, "mysqluser", "mysqlpass");

    assertNotNull(result);
    assertTrue(result.contains("CREATE USER 'mysqluser'@'%' IDENTIFIED BY 'mysqlpass'"));
  }

  @Test
  public void testGetGrantPrivilegesSqlPostgres() throws Exception {
    mockSetupArgs.databaseName = "testdb";
    mockSetupArgs.createUserUsername = "testuser";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;

    String result =
        createUsersStep.getGrantPrivilegesSql(DatabaseType.POSTGRES, "testuser", "testdb");

    assertNotNull(result);
    assertTrue(result.contains("GRANT ALL PRIVILEGES ON DATABASE \"testdb\" TO \"testuser\""));
  }

  @Test
  public void testGetGrantPrivilegesSqlMysql() throws Exception {
    mockSetupArgs.databaseName = "testdb";
    mockSetupArgs.createUserUsername = "testuser";
    mockSetupArgs.dbType = DatabaseType.MYSQL;

    String result = createUsersStep.getGrantPrivilegesSql(DatabaseType.MYSQL, "testuser", "testdb");

    assertNotNull(result);
    assertTrue(result.contains("GRANT ALL PRIVILEGES ON `testdb`.* TO 'testuser'@'%'"));
  }

  @Test
  public void testCreateUsersWithIamAuth() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = true;
    mockSetupArgs.createUserUsername = "iamuser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = createUsersStep.createUsers(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 1);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateUsersWithTraditionalAuth() throws SQLException {
    mockSetupArgs.createUser = true;
    mockSetupArgs.iamAuthEnabled = false;
    mockSetupArgs.createUserUsername = "traduser";
    mockSetupArgs.createUserPassword = "tradpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = createUsersStep.createUsers(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 1);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateUsersDisabled() throws SQLException {
    mockSetupArgs.createUser = false;

    SqlSetupResult result = createUsersStep.createUsers(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 0);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateIamUserWithException() throws SQLException {
    mockSetupArgs.createUserUsername = "iamuser";
    mockSetupArgs.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("User creation failed"));

    SqlSetupResult result = new SqlSetupResult();
    try {
      createUsersStep.createIamUser(mockSetupArgs, result);
      assertTrue(false, "Expected SQLException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testCreateTraditionalUserWithException() throws SQLException {
    mockSetupArgs.createUserUsername = "traduser";
    mockSetupArgs.createUserPassword = "tradpass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("User creation failed"));

    SqlSetupResult result = new SqlSetupResult();
    try {
      createUsersStep.createTraditionalUser(mockSetupArgs, result);
      assertTrue(false, "Expected SQLException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }
}
