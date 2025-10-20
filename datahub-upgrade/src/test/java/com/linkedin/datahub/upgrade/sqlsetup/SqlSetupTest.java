package com.linkedin.datahub.upgrade.sqlsetup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import io.ebean.Database;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SqlSetupTest {

  @Mock private Database mockDatabase;

  private SqlSetup sqlSetup;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSqlSetupInit() {
    SqlSetupArgs setupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
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
            "datahub", // databaseName
            null // createUserIamRole
            );
    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    assertNotNull(sqlSetup);
    assertEquals("SqlSetup", sqlSetup.id());
    assertTrue(sqlSetup.steps().size() >= 1);
    assertTrue(sqlSetup.cleanupSteps().isEmpty());
  }

  @Test
  public void testSqlSetupWithNullDatabase() {
    SqlSetupArgs setupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
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
            "datahub", // databaseName
            null // createUserIamRole
            );
    sqlSetup = new SqlSetup(null, setupArgs);

    assertNotNull(sqlSetup);
    assertEquals("SqlSetup", sqlSetup.id());
    assertEquals(sqlSetup.steps().size(), 0);
    assertTrue(sqlSetup.cleanupSteps().isEmpty());
  }

  @Test
  public void testSqlSetupWithNullArgs() {
    sqlSetup = new SqlSetup(mockDatabase, null);

    assertNotNull(sqlSetup);
    assertEquals("SqlSetup", sqlSetup.id());
    assertEquals(sqlSetup.steps().size(), 0);
    assertTrue(sqlSetup.cleanupSteps().isEmpty());
  }

  @Test
  public void testSqlSetupWithBothNull() {
    sqlSetup = new SqlSetup(null, null);

    assertNotNull(sqlSetup);
    assertEquals("SqlSetup", sqlSetup.id());
    assertEquals(sqlSetup.steps().size(), 0);
    assertTrue(sqlSetup.cleanupSteps().isEmpty());
  }

  @Test
  public void testBuildStepsWithCreateTablesOnly() {
    SqlSetupArgs setupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
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
            "datahub", // databaseName
            null // createUserIamRole
            );

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 1);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
  }

  @Test
  public void testBuildStepsWithCreateTablesAndUsers() {
    SqlSetupArgs setupArgs =
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
            "datahub", // databaseName
            null // createUserIamRole
            );

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 2);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateUsersStep);
  }

  @Test
  public void testBuildStepsWithCreateTablesAndCdc() {
    SqlSetupArgs setupArgs =
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
            "datahub", // databaseName
            null // createUserIamRole
            );

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 2);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateCdcUserStep);
  }

  @Test
  public void testBuildStepsWithAllEnabled() {
    SqlSetupArgs setupArgs =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            true, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            "testpass", // createUserPassword
            "localhost", // host
            3306, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 3);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateUsersStep);
    assertTrue(steps.get(2) instanceof CreateCdcUserStep);
  }
}
