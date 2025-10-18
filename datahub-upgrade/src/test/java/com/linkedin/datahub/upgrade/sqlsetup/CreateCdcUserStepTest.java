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
  @Mock private SqlSetupArgs mockSetupArgs;
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
    createCdcUserStep = new CreateCdcUserStep(mockDatabase, mockSetupArgs);
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
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "datahub_cdc";
    mockSetupArgs.cdcPassword = "cdc_password";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

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
    mockSetupArgs.cdcEnabled = false;

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
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
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "mysql_cdc";
    mockSetupArgs.cdcPassword = "mysql_cdc_pass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'mysql_cdc' created successfully");
  }

  @Test
  public void testExecutableWithPostgresCdc() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "postgres_cdc";
    mockSetupArgs.cdcPassword = "postgres_cdc_pass";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateCdcUserStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'postgres_cdc' created successfully");
  }

  @Test
  public void testExecutableWithException() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "datahub_cdc";
    mockSetupArgs.cdcPassword = "cdc_password";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

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
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "custom_cdc_user";
    mockSetupArgs.cdcPassword = "custom_cdc_password";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "custom_db";

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'custom_cdc_user' created successfully");
  }

  @Test
  public void testCdcUserCreationWithDefaultValues() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "datahub_cdc"; // Default value
    mockSetupArgs.cdcPassword = "datahub_cdc"; // Default value
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "datahub"; // Default value

    Function<UpgradeContext, UpgradeStepResult> executable = createCdcUserStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating CDC user...");
    verify(mockUpgradeReport).addLine("CDC user 'datahub_cdc' created successfully");
  }

  @Test
  public void testGetCreateCdcUserSqlPostgres() throws Exception {
    mockSetupArgs.cdcUser = "postgres_cdc";
    mockSetupArgs.cdcPassword = "postgres_cdc_pass";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;

    String result =
        createCdcUserStep.getCreateCdcUserSql(
            DatabaseType.POSTGRES, "postgres_cdc", "postgres_cdc_pass");

    assertNotNull(result);
    assertTrue(result.contains("DO"));
    assertTrue(result.contains("$$"));
    assertTrue(
        result.contains(
            "IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'postgres_cdc')"));
    assertTrue(result.contains("CREATE USER \"postgres_cdc\" WITH PASSWORD 'postgres_cdc_pass'"));
    assertTrue(result.contains("ALTER USER \"postgres_cdc\" WITH REPLICATION"));
  }

  @Test
  public void testGetCreateCdcUserSqlMysql() throws Exception {
    mockSetupArgs.cdcUser = "mysql_cdc";
    mockSetupArgs.cdcPassword = "mysql_cdc_pass";

    String result =
        createCdcUserStep.getCreateCdcUserSql(DatabaseType.MYSQL, "mysql_cdc", "mysql_cdc_pass");

    assertNotNull(result);
    assertTrue(
        result.contains(
            "CREATE USER IF NOT EXISTS 'mysql_cdc'@'%' IDENTIFIED BY 'mysql_cdc_pass'"));
  }

  @Test
  public void testGetGrantCdcPrivilegesSqlPostgres() throws Exception {
    mockSetupArgs.databaseName = "testdb";
    mockSetupArgs.cdcUser = "postgres_cdc";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;

    String result =
        createCdcUserStep.getGrantCdcPrivilegesSql(DatabaseType.POSTGRES, "postgres_cdc", "testdb");

    assertNotNull(result);
    assertTrue(result.contains("GRANT CONNECT ON DATABASE \"testdb\" TO \"postgres_cdc\""));
    assertTrue(result.contains("GRANT USAGE ON SCHEMA public TO \"postgres_cdc\""));
    assertTrue(result.contains("GRANT CREATE ON DATABASE \"testdb\" TO \"postgres_cdc\""));
    assertTrue(result.contains("GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"postgres_cdc\""));
    assertTrue(
        result.contains(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO \"postgres_cdc\""));
    assertTrue(result.contains("ALTER USER \"postgres_cdc\" WITH SUPERUSER"));
    assertTrue(result.contains("ALTER TABLE public.metadata_aspect_v2 OWNER TO \"postgres_cdc\""));
    assertTrue(result.contains("ALTER TABLE public.metadata_aspect_v2 REPLICA IDENTITY FULL"));
    assertTrue(
        result.contains("CREATE PUBLICATION dbz_publication FOR TABLE public.metadata_aspect_v2"));
  }

  @Test
  public void testGetGrantCdcPrivilegesSqlMysql() throws Exception {
    mockSetupArgs.databaseName = "testdb";
    mockSetupArgs.cdcUser = "mysql_cdc";

    String result =
        createCdcUserStep.getGrantCdcPrivilegesSql(DatabaseType.MYSQL, "mysql_cdc", "testdb");

    assertNotNull(result);
    assertTrue(result.contains("GRANT SELECT ON `testdb`.* TO 'mysql_cdc'@'%'"));
    assertTrue(result.contains("GRANT RELOAD ON *.* TO 'mysql_cdc'@'%'"));
    assertTrue(result.contains("GRANT REPLICATION CLIENT ON *.* TO 'mysql_cdc'@'%'"));
    assertTrue(result.contains("GRANT REPLICATION SLAVE ON *.* TO 'mysql_cdc'@'%'"));
    assertTrue(result.contains("FLUSH PRIVILEGES"));
  }

  @Test
  public void testCreateCdcUserWithException() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "test_cdc";
    mockSetupArgs.cdcPassword = "test_pass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock PreparedStatement.executeUpdate() to throw SQLException
    when(mockPreparedStatement.executeUpdate())
        .thenThrow(new SQLException("CDC user creation failed"));

    try {
      createCdcUserStep.createCdcUser(mockSetupArgs);
      assertTrue(false, "Expected SQLException to be thrown");
    } catch (Exception e) {
      assertTrue(e instanceof SQLException);
    }
  }

  @Test
  public void testCreateCdcUserSuccess() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "test_cdc";
    mockSetupArgs.cdcPassword = "test_pass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = createCdcUserStep.createCdcUser(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testCreateCdcUserDisabled() throws SQLException {
    mockSetupArgs.cdcEnabled = false;

    SqlSetupResult result = createCdcUserStep.createCdcUser(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), false);
    assertTrue(result.getExecutionTimeMs() >= 0);
    verify(mockDatabase, never()).sqlUpdate(anyString());
  }

  @Test
  public void testCreateCdcUserWithPostgresComplexSql() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "postgres_cdc";
    mockSetupArgs.cdcPassword = "postgres_cdc_pass";
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = createCdcUserStep.createCdcUser(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }

  @Test
  public void testCreateCdcUserWithMysqlSimpleSql() throws SQLException {
    mockSetupArgs.cdcEnabled = true;
    mockSetupArgs.cdcUser = "mysql_cdc";
    mockSetupArgs.cdcPassword = "mysql_cdc_pass";
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    SqlSetupResult result = createCdcUserStep.createCdcUser(mockSetupArgs);

    assertNotNull(result);
    assertEquals(result.isCdcUserCreated(), true);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // Verify PreparedStatement approach - should call dataSource() and prepareStatement()
    verify(mockDatabase).dataSource();
    verify(mockConnection, times(2)).prepareStatement(anyString());
    verify(mockPreparedStatement, times(2)).executeUpdate();
  }
}
