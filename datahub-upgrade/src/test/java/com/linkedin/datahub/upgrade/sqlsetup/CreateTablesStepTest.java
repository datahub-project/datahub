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

public class CreateTablesStepTest {

  @Mock private Database mockDatabase;
  @Mock private SqlSetupArgs mockSetupArgs;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;
  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private SqlUpdate mockSqlUpdate;

  private CreateTablesStep createTablesStep;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    createTablesStep = new CreateTablesStep(mockDatabase, mockSetupArgs);
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
    assertEquals(createTablesStep.id(), "CreateTablesStep");
  }

  @Test
  public void testRetryCount() {
    assertEquals(createTablesStep.retryCount(), 0);
  }

  @Test
  public void testExecutableSuccessWithMysql() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateTablesStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database tables...");
    verify(mockUpgradeReport).addLine(contains("Tables created:"));
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableSuccessWithPostgres() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false); // Database doesn't exist

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateTablesStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database tables...");
    verify(mockUpgradeReport).addLine(contains("Tables created:"));
    verify(mockUpgradeReport).addLine(contains("Execution time:"));
  }

  @Test
  public void testExecutableWithCreateDatabaseDisabled() throws SQLException {
    mockSetupArgs.createDatabase = false;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateTablesStep");
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(mockUpgradeReport).addLine("Creating database tables...");
    verify(mockUpgradeReport).addLine(contains("Tables created:"));
  }

  @Test
  public void testExecutableWithException() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock RuntimeException to be thrown
    doThrow(new RuntimeException("Database connection failed")).when(mockSqlUpdate).execute();

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.stepId(), "CreateTablesStep");
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    verify(mockUpgradeReport).addLine("Creating database tables...");
    verify(mockUpgradeReport).addLine(contains("Error during execution:"));
  }

  @Test
  public void testContainsKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));
    testMap.put("key2", java.util.Optional.of("value2"));
    testMap.put("key3", java.util.Optional.empty());

    // Test with existing key that has a value
    boolean result1 = createTablesStep.containsKey(testMap, "key1");
    assertTrue(result1);

    // Test with existing key that has empty optional
    boolean result2 = createTablesStep.containsKey(testMap, "key3");
    assertTrue(!result2);

    // Test with non-existing key
    boolean result3 = createTablesStep.containsKey(testMap, "key4");
    assertTrue(!result3);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testContainsKeyWithNullMap() {
    createTablesStep.containsKey(null, "key1");
  }

  @Test
  public void testContainsKeyWithNullKey() {
    java.util.Map<String, java.util.Optional<String>> testMap = new java.util.HashMap<>();
    testMap.put("key1", java.util.Optional.of("value1"));

    boolean result = createTablesStep.containsKey(testMap, null);
    assertTrue(!result);
  }

  @Test
  public void testCreateDatabaseIfNotExistsMysql() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that PreparedStatement calls were made for database creation
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).executeQuery();
    verify(mockConnection).prepareStatement(contains("CREATE DATABASE"));
    verify(mockConnection).prepareStatement("USE `testdb`");
    verify(mockPreparedStatement, times(2))
        .executeUpdate(); // Once for CREATE DATABASE, once for USE
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgres() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false); // Database doesn't exist

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that table creation SQL was executed
    verify(mockDatabase, atLeastOnce()).sqlUpdate(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test
  public void testGetCreateTableSqlPostgres() throws Exception {
    String result = createTablesStep.getCreateTableSql(DatabaseType.POSTGRES);

    assertNotNull(result);
    assertTrue(result.contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(result.contains("varchar(500)"));
    assertTrue(result.contains("text"));
    assertTrue(result.contains("timestamp"));
    assertTrue(result.contains("CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY"));
    assertTrue(result.contains("CREATE INDEX IF NOT EXISTS timeIndex"));
  }

  @Test
  public void testGetCreateTableSqlMysql() throws Exception {
    String result = createTablesStep.getCreateTableSql(DatabaseType.MYSQL);

    assertNotNull(result);
    assertTrue(result.contains("CREATE TABLE IF NOT EXISTS metadata_aspect_v2"));
    assertTrue(result.contains("varchar(500)"));
    assertTrue(result.contains("longtext"));
    assertTrue(result.contains("datetime(6)"));
    assertTrue(result.contains("CONSTRAINT pk_metadata_aspect_v2 PRIMARY KEY"));
    assertTrue(result.contains("INDEX timeIndex"));
    assertTrue(result.contains("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin"));
  }

  @Test
  public void testSelectDatabase() throws SQLException {
    createTablesStep.selectDatabase("testdb");

    verify(mockConnection).prepareStatement("USE `testdb`");
    verify(mockPreparedStatement).executeUpdate();
  }

  @Test
  public void testCreatePostgresDatabaseDirectly() throws SQLException {
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false); // Database doesn't exist

    createTablesStep.createPostgresDatabaseDirectly("testdb");

    verify(mockConnection).setAutoCommit(true);
    verify(mockPreparedStatement).executeQuery();
    verify(mockPreparedStatement).executeUpdate();
  }

  @Test
  public void testCreatePostgresDatabaseDirectlyDatabaseExists() throws SQLException {
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true); // Database exists

    createTablesStep.createPostgresDatabaseDirectly("testdb");

    verify(mockConnection).setAutoCommit(true);
    verify(mockPreparedStatement).executeQuery();
    verify(mockPreparedStatement, never()).executeUpdate();
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgresDatabaseExists() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    // Mock that database exists (ResultSet.next() returns true)
    when(mockResultSet.next()).thenReturn(true);

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection).prepareStatement(contains("SELECT 1 FROM pg_database"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).executeQuery();
  }

  @Test
  public void testCreateDatabaseIfNotExistsMysqlDatabaseExists() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock that database exists (ResultSet.next() returns true)
    when(mockResultSet.next()).thenReturn(true);

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).executeQuery();
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgresDatabaseCheckFails() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.POSTGRES;
    mockSetupArgs.databaseName = "testdb";

    // Mock database check failure - PreparedStatement throws exception
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Check failed"));

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection).prepareStatement(contains("SELECT 1 FROM pg_database"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).executeQuery();
  }

  @Test
  public void testCreateDatabaseIfNotExistsMysqlDatabaseCheckFails() throws SQLException {
    mockSetupArgs.createDatabase = true;
    mockSetupArgs.dbType = DatabaseType.MYSQL;
    mockSetupArgs.databaseName = "testdb";

    // Mock database check failure - PreparedStatement throws exception
    when(mockPreparedStatement.executeQuery()).thenThrow(new SQLException("Check failed"));

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).executeQuery();
  }
}
