package com.linkedin.datahub.upgrade.cleanup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DropDatabaseStepTest {

  @Mock private Database mockDatabase;
  @Mock private DataSource mockDataSource;
  @Mock private Connection mockConnection;
  @Mock private Statement mockStatement;
  @Mock private PreparedStatement mockPreparedStatement;

  private UpgradeContext mockContext;

  @BeforeMethod
  public void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    mockContext = mock(UpgradeContext.class);
    when(mockDatabase.dataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
  }

  private SqlSetupArgs makeArgs(
      DatabaseType dbType,
      String dbName,
      boolean createUser,
      String createUserUsername,
      boolean cdcEnabled,
      String cdcUser) {
    return new SqlSetupArgs(
        true,
        true,
        createUser,
        false,
        dbType,
        cdcEnabled,
        cdcUser,
        "cdcpass",
        createUserUsername,
        "userpass",
        "localhost",
        dbType == DatabaseType.POSTGRES ? 5432 : 3306,
        dbName,
        false);
  }

  @Test
  public void testId() {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);
    assertEquals(step.id(), "DropDatabaseStep");
  }

  @Test
  public void testRetryCount() {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);
    assertEquals(step.retryCount(), 1);
  }

  @Test
  public void testSucceedsWithNullDatabaseName() {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, null, false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertNotNull(result);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSucceedsWithEmptyDatabaseName() {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testDropsMysqlDatabase() throws SQLException {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockStatement).execute(contains("DROP DATABASE IF EXISTS `testdb`"));
  }

  @Test
  public void testDropsPostgresDatabase() throws SQLException {
    SqlSetupArgs args = makeArgs(DatabaseType.POSTGRES, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // pg_terminate_backend uses a PreparedStatement to avoid SQL injection
    verify(mockPreparedStatement).setString(1, "testdb");
    verify(mockPreparedStatement).execute();
    verify(mockStatement).execute(contains("DROP DATABASE IF EXISTS \"testdb\""));
  }

  @Test
  public void testDropsAppUserWhenConfigured() throws SQLException {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", true, "datahub_user", false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockStatement).execute(contains("DROP USER IF EXISTS 'datahub_user'"));
  }

  @Test
  public void testDropsCdcUserWhenConfigured() throws SQLException {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, true, "cdc_user");
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockStatement).execute(contains("DROP USER IF EXISTS 'cdc_user'"));
  }

  @Test
  public void testDropsBothUsersWhenConfigured() throws SQLException {
    SqlSetupArgs args =
        makeArgs(DatabaseType.POSTGRES, "testdb", true, "app_user", true, "cdc_user");
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockStatement).execute(contains("DROP USER IF EXISTS \"app_user\""));
    verify(mockStatement).execute(contains("DROP USER IF EXISTS \"cdc_user\""));
  }

  @Test
  public void testSkipsUserDropWhenUsernameEmpty() throws SQLException {
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", true, "", false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Only the DROP DATABASE should be executed, no DROP USER
    verify(mockStatement, times(1)).execute(anyString());
  }

  @Test
  public void testFailsWhenConnectionThrows() throws SQLException {
    when(mockDataSource.getConnection()).thenThrow(new SQLException("Connection refused"));
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testFailsWhenDropDatabaseThrows() throws SQLException {
    when(mockStatement.execute(contains("DROP DATABASE")))
        .thenThrow(new SQLException("Permission denied"));
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testContinuesWhenUserDropFails() throws SQLException {
    // User drop failure is logged as warning but doesn't fail the step
    when(mockStatement.execute(contains("DROP USER")))
        .thenThrow(new SQLException("User does not exist"));
    SqlSetupArgs args = makeArgs(DatabaseType.MYSQL, "testdb", true, "app_user", false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testPostgresTerminateConnectionsFailureDoesNotBlock() throws SQLException {
    // pg_terminate_backend uses PreparedStatement; failure is swallowed, DROP DATABASE proceeds
    when(mockPreparedStatement.execute()).thenThrow(new SQLException("No permission to terminate"));
    SqlSetupArgs args = makeArgs(DatabaseType.POSTGRES, "testdb", false, null, false, null);
    DropDatabaseStep step = new DropDatabaseStep(mockDatabase, args);

    UpgradeStepResult result = step.executable().apply(mockContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockStatement).execute(contains("DROP DATABASE IF EXISTS \"testdb\""));
  }
}
