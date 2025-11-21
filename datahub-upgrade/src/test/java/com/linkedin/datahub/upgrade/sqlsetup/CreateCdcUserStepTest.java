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

public class CreateCdcUserStepTest {

  @Mock private Database mockDatabase;
  @Mock private SqlUpdate mockSqlUpdate;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;
  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;

  private CreateCdcUserStep createCdcUserStep;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    SqlSetupArgs defaultSetupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            false, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            true, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            null, // createUserUsername
            null, // createUserPassword
            "localhost", // host
            3306, // port
            "testdb" // databaseName
            );
    createCdcUserStep = new CreateCdcUserStep(mockDatabase, defaultSetupArgs);
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
    assertEquals(createCdcUserStep.id(), "CreateCdcUserStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(createCdcUserStep.retryCount(), 0);
  }

  @Test
  public void testExecutableSuccessWithCdcEnabled() throws SQLException {

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'datahub_cdc' created successfully");
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableWithCdcDisabled() throws SQLException {
    // Create a CreateCdcUserStep with CDC disabled
    SqlSetupArgs disabledCdcArgs =
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
            "testdb");
    CreateCdcUserStep disabledCdcStep = new CreateCdcUserStep(mockDatabase, disabledCdcArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = disabledCdcStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user creation skipped or failed");
  }

  @Test
  public void testExecutableWithMysqlCdc() throws SQLException {
    // Create a CreateCdcUserStep with MySQL CDC user
    SqlSetupArgs mysqlCdcArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "mysql_cdc",
            "mysql_cdc_pass",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    CreateCdcUserStep mysqlCdcStep = new CreateCdcUserStep(mockDatabase, mysqlCdcArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = mysqlCdcStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'mysql_cdc' created successfully");
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableWithPostgresCdc() throws SQLException {
    // Create a CreateCdcUserStep with PostgreSQL CDC user
    SqlSetupArgs postgresCdcArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.POSTGRES,
            true,
            "postgres_cdc",
            "postgres_cdc_pass",
            null,
            null,
            "localhost",
            5432,
            "testdb");
    CreateCdcUserStep postgresCdcStep = new CreateCdcUserStep(mockDatabase, postgresCdcArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = postgresCdcStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'postgres_cdc' created successfully");
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableWithException() throws SQLException {

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("CDC user creation failed"));

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine(contains("Error during execution:"));
  }

  @Test
  public void testContainsKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));
    testMap.put("key2", java.util.Optional.of("value2"));
    testMap.put("key3", java.util.Optional.empty());

    // Test with existing key that has a value
    boolean result1 = createCdcUserStep.containsKey(testMap, "key1");
    assertTrue(result1);

    // Test with existing key that has empty optional
    boolean result2 = createCdcUserStep.containsKey(testMap, "key3");
    assertTrue(!result2);

    // Test with non-existing key
    boolean result3 = createCdcUserStep.containsKey(testMap, "key4");
    assertTrue(!result3);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testContainsKeyWithNullMap() {
    createCdcUserStep.containsKey(null, "key1");
  }

  @Test
  public void testContainsKeyWithNullKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));

    boolean result = createCdcUserStep.containsKey(testMap, null);
    assertTrue(!result);
  }

  @Test
  public void testCdcUserCreationWithCustomValues() throws SQLException {
    // Create a CreateCdcUserStep with custom CDC user
    SqlSetupArgs customCdcArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.POSTGRES,
            true,
            "custom_cdc_user",
            "custom_cdc_password",
            null,
            null,
            "localhost",
            5432,
            "custom_db");
    CreateCdcUserStep customCdcStep = new CreateCdcUserStep(mockDatabase, customCdcArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = customCdcStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'custom_cdc_user' created successfully");
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testCdcUserCreationWithDefaultValues() throws SQLException {
    // Create a CreateCdcUserStep with default CDC user
    SqlSetupArgs defaultCdcArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    CreateCdcUserStep defaultCdcStep = new CreateCdcUserStep(mockDatabase, defaultCdcArgs);

    Function<UpgradeContext, UpgradeStepResult> executable = defaultCdcStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'datahub_cdc' created successfully");
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testGetCreateCdcUserSqlPostgres() throws Exception {
    SqlSetupArgs postgresArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.POSTGRES,
            true,
            "postgres_cdc",
            "postgres_cdc_pass",
            null,
            null,
            "localhost",
            5432,
            "testdb");
    CreateCdcUserStep postgresStep = new CreateCdcUserStep(mockDatabase, postgresArgs);

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = postgresStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetCreateCdcUserSqlMysql() throws Exception {
    SqlSetupArgs mysqlArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "mysql_cdc",
            "mysql_cdc_pass",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    CreateCdcUserStep mysqlStep = new CreateCdcUserStep(mockDatabase, mysqlArgs);

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = mysqlStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetGrantCdcPrivilegesSqlPostgres() throws Exception {
    SqlSetupArgs postgresArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.POSTGRES,
            true,
            "postgres_cdc",
            "postgres_cdc_pass",
            null,
            null,
            "localhost",
            5432,
            "testdb");
    CreateCdcUserStep postgresStep = new CreateCdcUserStep(mockDatabase, postgresArgs);

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = postgresStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetGrantCdcPrivilegesSqlMysql() throws Exception {
    SqlSetupArgs mysqlArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "mysql_cdc",
            "mysql_cdc_pass",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    CreateCdcUserStep mysqlStep = new CreateCdcUserStep(mockDatabase, mysqlArgs);

    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = mysqlStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCreateCdcUserWithException() throws SQLException {

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("CDC user creation failed"));

    try {
      SqlSetupArgs testArgs =
          new SqlSetupArgs(
              true,
              true,
              false,
              false,
              DatabaseType.MYSQL,
              true,
              "datahub_cdc",
              "datahub_cdc",
              null,
              null,
              "localhost",
              3306,
              "testdb");
      createCdcUserStep.createCdcUser(testArgs);
      assertTrue(false, "Expected RuntimeException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof RuntimeException);
      assertTrue(e.getMessage().contains("Failed to create CDC user"));
      assertTrue(e.getCause() instanceof SQLException);
    }
  }

  @Test
  public void testCreateCdcUserSuccess() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    SqlSetupResult result = createCdcUserStep.createCdcUser(testArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    // MySQL: 1 createCdcUser + 5 grantCdcPrivileges statements = 6 total
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(6)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(6)).executeUpdate();
  }

  @Test
  public void testCreateCdcUserDisabled() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            false, // CDC disabled
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    SqlSetupResult result = createCdcUserStep.createCdcUser(testArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), false);
    assertTrue(result.getExecutionTimeMs() >= 0);
    verify(mockDatabase, never()).sqlUpdate(anyString());
  }

  @Test
  public void testCreateCdcUserWithPostgresComplexSql() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    SqlSetupResult result = createCdcUserStep.createCdcUser(testArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    // MySQL: 1 createCdcUser + 5 grantCdcPrivileges statements = 6 total
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(6)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(6)).executeUpdate();
  }

  @Test
  public void testCreateCdcUserWithMysqlSimpleSql() throws SQLException {

    SqlSetupArgs testArgs =
        new SqlSetupArgs(
            true,
            true,
            false,
            false,
            DatabaseType.MYSQL,
            true,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            3306,
            "testdb");
    SqlSetupResult result = createCdcUserStep.createCdcUser(testArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    // MySQL: 1 createCdcUser + 5 grantCdcPrivileges statements = 6 total
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(6)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(6)).executeUpdate();
  }
}
