package com.linkedin.datahub.upgrade.sqlsetup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeReport;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import io.ebean.SqlUpdate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Function;
import javax.sql.DataSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateTablesStepTest {

  @Mock private Database mockDatabase;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private UpgradeReport mockUpgradeReport;
  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private Statement mockStatement;
  @Mock private SqlUpdate mockSqlUpdate;

  private CreateTablesStep createTablesStep;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    reset(
        mockDatabase,
        mockDataSource,
        mockConnection,
        mockPreparedStatement,
        mockResultSet,
        mockStatement,
        mockSqlUpdate,
        mockUpgradeContext,
        mockUpgradeReport);
    SqlSetupArgs defaultSetupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createSchema
            false, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            null, // createUserUsername
            null, // createUserPassword
            "localhost", // host
            3306, // port
            "testdb", // databaseName
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);
    createTablesStep = new CreateTablesStep(mockDatabase, defaultSetupArgs);
    when(mockUpgradeContext.report()).thenReturn(mockUpgradeReport);

    // Setup mock DataSource chain for PreparedStatement approach
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
    when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockPreparedStatement.executeUpdate()).thenReturn(1);
    when(mockResultSet.next()).thenReturn(false); // Default: database doesn't exist
    when(mockConnection.getAutoCommit()).thenReturn(true);

    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockStatement.execute(anyString())).thenReturn(false);

    // Setup mock to return SqlUpdate mock when sqlUpdate is called (for table creation)
    when(mockDatabase.sqlUpdate(anyString())).thenReturn(mockSqlUpdate);
    when(mockSqlUpdate.execute()).thenReturn(1); // Return 1 for successful execution
  }

  /**
   * Stubs JDBC prepareStatement responses for Postgres SqlSetup: database existence, optional
   * schema existence, current_schema() verification, and postSetup invalid-index checks.
   */
  private static void stubPostgresPreparedStatements(
      Connection connection, String metadataSchema, boolean schemaExists) throws SQLException {
    when(connection.getAutoCommit()).thenReturn(true);
    when(connection.prepareStatement(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              PreparedStatement ps = mock(PreparedStatement.class);
              ResultSet rs = mock(ResultSet.class);
              when(ps.executeQuery()).thenReturn(rs);
              when(ps.executeUpdate()).thenReturn(1);
              if (sql.contains("current_schema()")) {
                when(rs.next()).thenReturn(true);
                when(rs.getString(1)).thenReturn(metadataSchema);
              } else if (sql.contains("pg_namespace")) {
                when(rs.next()).thenReturn(schemaExists);
              } else if (sql.contains("pg_database")) {
                when(rs.next()).thenReturn(false);
              } else {
                // postSetup invalid-index probe, etc.
                when(rs.next()).thenReturn(false);
              }
              return ps;
            });
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
    // Create a new CreateTablesStep with PostgreSQL configuration
    SqlSetupArgs postgresSetupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createSchema
            false, // createUser
            false, // iamAuthEnabled
            DatabaseType.POSTGRES, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            null, // createUserUsername
            null, // createUserPassword
            "localhost", // host
            5432, // port
            "testdb", // databaseName
            "public", // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);
    CreateTablesStep postgresStep = new CreateTablesStep(mockDatabase, postgresSetupArgs);
    stubPostgresPreparedStatements(mockConnection, "public", true);

    Function<UpgradeContext, UpgradeStepResult> executable = postgresStep.executable();
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

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    assertNotNull(executable);

    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify that PreparedStatement calls were made for database creation
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement, times(7))
        .executeQuery(); // schema + 3 legacy drops + 2 ensureAspectIndexes + 1 collation check
    verify(mockConnection).prepareStatement(contains("CREATE DATABASE"));
    verify(mockConnection).prepareStatement("USE `testdb`");
    verify(mockPreparedStatement, times(2))
        .executeUpdate(); // Once for CREATE DATABASE, once for USE
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgres() throws SQLException {
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
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testGetCreateTableSqlMysql() throws Exception {
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSelectDatabase() throws SQLException {
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCreatePostgresDatabaseDirectly() throws SQLException {
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCreatePostgresDatabaseDirectlyDatabaseExists() throws SQLException {
    // This test is now covered by DatabaseOperationsTest
    // Testing the step execution instead
    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgresDatabaseExists() throws SQLException {
    // Create a CreateTablesStep with PostgreSQL database
    SqlSetupArgs postgresArgs =
        new SqlSetupArgs(
            true,
            true,
            true, // createSchema
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "testdb",
            false,
            null);
    CreateTablesStep postgresStep = new CreateTablesStep(mockDatabase, postgresArgs);
    // Override pg_database existence to true (schema verify still returns testdb)
    when(mockConnection.prepareStatement(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              PreparedStatement ps = mock(PreparedStatement.class);
              ResultSet rs = mock(ResultSet.class);
              when(ps.executeQuery()).thenReturn(rs);
              when(ps.executeUpdate()).thenReturn(1);
              if (sql.contains("current_schema()")) {
                when(rs.next()).thenReturn(true);
                when(rs.getString(1)).thenReturn("testdb");
              } else if (sql.contains("pg_database")) {
                when(rs.next()).thenReturn(true);
              } else if (sql.contains("pg_namespace")) {
                when(rs.next()).thenReturn(true);
              } else {
                when(rs.next()).thenReturn(false);
              }
              return ps;
            });

    Function<UpgradeContext, UpgradeStepResult> executable = postgresStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection).prepareStatement(contains("SELECT 1 FROM pg_database"));
  }

  @Test
  public void testCreateDatabaseIfNotExistsMysqlDatabaseExists() throws SQLException {

    // Mock that database exists (ResultSet.next() returns true)
    when(mockResultSet.next()).thenReturn(true);
    // Legacy index counts 0 (nothing to drop); timeIndex + idx_version_urn_aspect already present;
    // final 0 = key columns already utf8mb4_bin (no collation conversion needed)
    when(mockResultSet.getInt(1)).thenReturn(0, 0, 0, 1, 1, 0);

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement, times(7))
        .executeQuery(); // schema + 3 legacy + 2 ensureAspectIndexes + 1 collation check
  }

  @Test
  public void testCreateDatabaseIfNotExistsPostgresDatabaseCheckFails() throws SQLException {
    // Create a CreateTablesStep with PostgreSQL database
    SqlSetupArgs postgresArgs =
        new SqlSetupArgs(
            true,
            true,
            true, // createSchema
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "testdb",
            false,
            null);
    CreateTablesStep postgresStep = new CreateTablesStep(mockDatabase, postgresArgs);
    when(mockConnection.prepareStatement(anyString()))
        .thenAnswer(
            invocation -> {
              String sql = invocation.getArgument(0);
              PreparedStatement ps = mock(PreparedStatement.class);
              if (sql.contains("pg_database")) {
                when(ps.executeQuery()).thenThrow(new SQLException("Check failed"));
                return ps;
              }
              ResultSet rs = mock(ResultSet.class);
              when(ps.executeQuery()).thenReturn(rs);
              when(ps.executeUpdate()).thenReturn(1);
              if (sql.contains("current_schema()")) {
                when(rs.next()).thenReturn(true);
                when(rs.getString(1)).thenReturn("testdb");
              } else if (sql.contains("pg_namespace")) {
                // CREATE SCHEMA ran on the table connection; later setSearchPath checks existence.
                when(rs.next()).thenReturn(true);
              } else {
                when(rs.next()).thenReturn(false);
              }
              return ps;
            });

    Function<UpgradeContext, UpgradeStepResult> executable = postgresStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection).prepareStatement(contains("SELECT 1 FROM pg_database"));
  }

  @Test
  public void testCreateDatabaseIfNotExistsMysqlDatabaseCheckFails() throws SQLException {

    // First executeQuery (schema existence check) fails; fallback create path runs.
    // Subsequent executeQuery calls are from dropLegacyAspectTableIndexes and must succeed.
    when(mockPreparedStatement.executeQuery())
        .thenThrow(new SQLException("Check failed"))
        .thenReturn(mockResultSet)
        .thenReturn(mockResultSet)
        .thenReturn(mockResultSet)
        .thenReturn(mockResultSet)
        .thenReturn(mockResultSet)
        .thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    Function<UpgradeContext, UpgradeStepResult> executable = createTablesStep.executable();
    UpgradeStepResult result = executable.apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockConnection)
        .prepareStatement(contains("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"));
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement, times(7)).executeQuery();
  }

  @Test
  public void testCreateTablesMysqlAcquiresFiveConnectionsWhenCreateDatabaseEnabled()
      throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            true,
            true, // createSchema
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
            null,
            false,
            null);
    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);

    SqlSetupResult result = step.createTables(args);

    assertEquals(result.getTablesCreated(), 1);
    assertTrue(result.getExecutionTimeMs() >= 0);
    // createDatabase + selectDatabase + dropLegacyIndexes + ensureAspectIndexes + ensureCollation
    verify(mockDataSource, times(5)).getConnection();
  }

  @Test
  public void testCreateTablesMysqlAcquiresFourConnectionsWhenCreateDatabaseDisabled()
      throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            false,
            true, // createSchema
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
            null,
            false,
            null);
    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);

    step.createTables(args);

    // selectDatabase + dropLegacyIndexes + ensureAspectIndexes + ensureCollation
    verify(mockDataSource, times(4)).getConnection();
  }

  @Test
  public void testCreateTablesPostgresRunsConcurrentDropAndEnsureIndexStatements()
      throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            true,
            true, // createSchema
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "testdb",
            false,
            null);

    Connection createDbConn = mock(Connection.class);
    Connection selectConn = mock(Connection.class);
    Connection schemaAndTableConn = mock(Connection.class);
    Connection dropLegacyConn = mock(Connection.class);
    Connection ensureIndexesConn = mock(Connection.class);
    Connection collationConn = mock(Connection.class);
    Statement schemaAndTableStmt = mock(Statement.class);
    Statement dropLegacyStmt = mock(Statement.class);
    Statement ensureIndexesStmt = mock(Statement.class);
    Statement collationStmt = mock(Statement.class);

    when(mockDataSource.getConnection())
        .thenReturn(createDbConn)
        .thenReturn(selectConn)
        .thenReturn(schemaAndTableConn)
        .thenReturn(dropLegacyConn)
        .thenReturn(ensureIndexesConn)
        .thenReturn(collationConn);

    stubPostgresPreparedStatements(createDbConn, "testdb", true);
    stubPostgresPreparedStatements(selectConn, "testdb", true);
    stubPostgresPreparedStatements(schemaAndTableConn, "testdb", true);
    stubPostgresPreparedStatements(dropLegacyConn, "testdb", true);
    stubPostgresPreparedStatements(ensureIndexesConn, "testdb", true);
    stubPostgresPreparedStatements(collationConn, "testdb", true);

    when(schemaAndTableConn.createStatement()).thenReturn(schemaAndTableStmt);
    when(dropLegacyConn.createStatement()).thenReturn(dropLegacyStmt);
    when(ensureIndexesConn.createStatement()).thenReturn(ensureIndexesStmt);
    when(collationConn.createStatement()).thenReturn(collationStmt);
    when(schemaAndTableStmt.execute(anyString())).thenReturn(false);
    when(dropLegacyStmt.execute(anyString())).thenReturn(false);
    when(ensureIndexesStmt.execute(anyString())).thenReturn(false);

    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);
    step.createTables(args);

    // CREATE TABLE must share the schema/search_path connection (not Ebean sqlUpdate / other
    // pool connections).
    verify(mockDatabase, never()).sqlUpdate(anyString());
    verify(schemaAndTableStmt).execute("CREATE SCHEMA IF NOT EXISTS testdb");
    verify(schemaAndTableStmt).execute("SET search_path TO testdb, public");
    verify(schemaAndTableStmt).execute(contains("CREATE TABLE IF NOT EXISTS"));
    verify(dropLegacyStmt).execute("SET search_path TO testdb, public");
    verify(dropLegacyStmt).execute("DROP INDEX CONCURRENTLY IF EXISTS urnindex");
    verify(dropLegacyStmt).execute("DROP INDEX CONCURRENTLY IF EXISTS aspectindex");
    verify(dropLegacyStmt).execute("DROP INDEX CONCURRENTLY IF EXISTS versionindex");
    verify(dropLegacyStmt, never()).execute(contains("CREATE TABLE"));
    verify(ensureIndexesStmt).execute("SET search_path TO testdb, public");
    verify(ensureIndexesStmt)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS timeIndex ON metadata_aspect_v2 (createdon);");
    verify(ensureIndexesStmt)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_v0_urn_aspect ON metadata_aspect_v2 (urn, aspect) WHERE version = 0;");
    verify(ensureIndexesStmt)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_corpuser_aspect_v0 ON metadata_aspect_v2 (urn, aspect) WHERE urn LIKE 'urn:li:corpuser:%' AND version = 0;");
    verify(ensureIndexesStmt)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_corpgroup_aspect_v0 ON metadata_aspect_v2 (urn, aspect) WHERE urn LIKE 'urn:li:corpGroup:%' AND version = 0;");
    verify(ensureIndexesStmt, never()).execute(contains("CREATE TABLE"));
    verify(collationStmt).execute("SET search_path TO testdb, public");
  }

  @Test
  public void testCreateTablesPostgresSkipsCreateSchemaWhenDisabled() throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            true,
            false, // createSchema — pre-provisioned
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "dhub",
            false,
            null);
    stubPostgresPreparedStatements(mockConnection, "dhub", true);
    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);

    step.createTables(args);

    verify(mockStatement, never()).execute(contains("CREATE SCHEMA"));
    verify(mockStatement, atLeastOnce()).execute("SET search_path TO dhub, public");
    verify(mockStatement).execute(contains("CREATE TABLE IF NOT EXISTS"));
  }

  @Test
  public void testCreateTablesPostgresFailsWhenPreProvisionedSchemaMissing() throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            false,
            false, // createSchema — pre-provisioned but missing
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "dhub",
            false,
            null);
    stubPostgresPreparedStatements(mockConnection, "dhub", false);
    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);

    try {
      step.createTables(args);
      throw new AssertionError("Expected SQLException for missing pre-provisioned schema");
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("does not exist"));
    }
    verify(mockStatement, never()).execute(contains("CREATE TABLE"));
  }

  @Test
  public void testCreateTablesPostgresWithSchemaVersionAcquiresSeventhConnectionAndRunsPostSetup()
      throws SQLException {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true,
            true,
            true, // createSchema
            false,
            false,
            DatabaseType.POSTGRES,
            false,
            "datahub_cdc",
            "datahub_cdc",
            null,
            null,
            "localhost",
            5432,
            "testdb",
            "testdb",
            true,
            null);
    stubPostgresPreparedStatements(mockConnection, "testdb", true);
    CreateTablesStep step = new CreateTablesStep(mockDatabase, args);

    step.createTables(args);

    // createDatabase + selectDatabase + ensureSchema/createTables + dropLegacyIndexes
    // + ensureAspectIndexes + ensureCollation + postSetup
    verify(mockDataSource, times(7)).getConnection();
    // ensureSchema+CREATE TABLE (3) + setSearchPath+3 drops (4) + setSearchPath+4 indexes (5)
    // + setSearchPath for collation (1) + setSearchPath+CREATE INDEX (2) = 15
    verify(mockStatement, times(15)).execute(anyString());
    verify(mockStatement)
        .execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS schemaVersionIndex ON metadata_aspect_v2 ((systemmetadata::jsonb ->> 'schemaVersion'));");
  }
}
