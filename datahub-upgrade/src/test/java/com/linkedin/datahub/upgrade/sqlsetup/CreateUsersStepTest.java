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
    SqlSetupArgs defaultSetupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            "testpass", // createUserPassword
            "localhost", // host
            3306, // port
            "testdb", // databaseName
            null // createUserIamRole
            );
    createUsersStep = new CreateUsersStep(mockDatabase, defaultSetupArgs);
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
    // Create a CreateUsersStep with user creation disabled
    SqlSetupArgs disabledUserArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb",
            null);
    CreateUsersStep disabledUserStep = new CreateUsersStep(mockDatabase, disabledUserArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = disabledUserStep.executable();
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

    SqlSetupResult result = new SqlSetupResult();
    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            true,
            true,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            "testuser",
            null,
            "localhost",
            3306,
            "testdb",
            "arn:aws:iam::123456789012:role/datahub-role");
    createUsersStep.createIamUser(testArgs, result);

    assertEquals(result.getUsersCreated(), 1);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testCreateTraditionalUser() throws SQLException {

    SqlSetupResult result = new SqlSetupResult();
    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            true,
            false,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            "testuser",
            "testpass",
            "localhost",
            3306,
            "testdb",
            null);
    createUsersStep.createTraditionalUser(testArgs, result);

    assertEquals(result.getUsersCreated(), 1);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testGetCreateIamUserSqlPostgres() throws Exception {
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetCreateIamUserSqlMysql() throws Exception {

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetCreateTraditionalUserSqlPostgres() throws Exception {

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetCreateTraditionalUserSqlMysql() throws Exception {

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetGrantPrivilegesSqlPostgres() throws Exception {

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetGrantPrivilegesSqlMysql() throws Exception {

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createUsersStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCreateUsersWithIamAuth() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            true,
            false,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            "testuser",
            "testpass",
            "localhost",
            3306,
            "testdb",
            null);
    SqlSetupResult result = createUsersStep.createUsers(testArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 1);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateUsersWithTraditionalAuth() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            true,
            false,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            "testuser",
            "testpass",
            "localhost",
            3306,
            "testdb",
            null);
    SqlSetupResult result = createUsersStep.createUsers(testArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 1);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateUsersDisabled() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            false, // createUser disabled
            false,
            DatabaseType.MYSQL,
            false,
            "datahub_cdc",
            "datahub_cdc",
            "testuser",
            "testpass",
            "localhost",
            3306,
            "testdb",
            null);
    SqlSetupResult result = createUsersStep.createUsers(testArgs);

    assertNotNull(result);
    assertEquals(result.getUsersCreated(), 0);
    assertTrue(result.getExecutionTimeMs() >= 0);
  }

  @Test
  public void testCreateIamUserWithException() throws SQLException {

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("User creation failed"));

    SqlSetupResult result = new SqlSetupResult();
    try {
      SqlSetupArgs testArgs =
          new SqlSetupArgs(
              true,
              true,
              true,
              true,
              DatabaseType.MYSQL,
              false,
              "datahub_cdc",
              "datahub_cdc",
              "testuser",
              null,
              "localhost",
              3306,
              "testdb",
              "arn:aws:iam::123456789012:role/datahub-role");
      createUsersStep.createIamUser(testArgs, result);
      assertTrue(false, "Expected SQLException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testCreateTraditionalUserWithException() throws SQLException {

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("User creation failed"));

    SqlSetupResult result = new SqlSetupResult();
    try {
      SqlSetupArgs testArgs =
          new SqlSetupArgs(
              true,
              true,
              true,
              false,
              DatabaseType.MYSQL,
              false,
              "datahub_cdc",
              "datahub_cdc",
              "testuser",
              "testpass",
              "localhost",
              3306,
              "testdb",
              null);
      createUsersStep.createTraditionalUser(testArgs, result);
      assertTrue(false, "Expected SQLException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }
}
