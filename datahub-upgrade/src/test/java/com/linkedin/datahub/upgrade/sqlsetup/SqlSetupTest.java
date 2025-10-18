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
  @Mock private SqlSetupArgs mockSetupArgs;

  private SqlSetup sqlSetup;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSqlSetupInit() {
    sqlSetup = new SqlSetup(mockDatabase, mockSetupArgs);

    assertNotNull(sqlSetup);
    assertEquals("SqlSetup", sqlSetup.id());
    assertTrue(sqlSetup.steps().size() >= 1);
    assertTrue(sqlSetup.cleanupSteps().isEmpty());
  }

  @Test
  public void testSqlSetupWithNullDatabase() {
    sqlSetup = new SqlSetup(null, mockSetupArgs);

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
    mockSetupArgs.createTables = true;
    mockSetupArgs.createUser = false;
    mockSetupArgs.cdcEnabled = false;

    sqlSetup = new SqlSetup(mockDatabase, mockSetupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 1);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
  }

  @Test
  public void testBuildStepsWithCreateTablesAndUsers() {
    mockSetupArgs.createTables = true;
    mockSetupArgs.createUser = true;
    mockSetupArgs.cdcEnabled = false;

    sqlSetup = new SqlSetup(mockDatabase, mockSetupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 2);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateUsersStep);
  }

  @Test
  public void testBuildStepsWithCreateTablesAndCdc() {
    mockSetupArgs.createTables = true;
    mockSetupArgs.createUser = false;
    mockSetupArgs.cdcEnabled = true;

    sqlSetup = new SqlSetup(mockDatabase, mockSetupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 2);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateCdcUserStep);
  }

  @Test
  public void testBuildStepsWithAllEnabled() {
    mockSetupArgs.createTables = true;
    mockSetupArgs.createUser = true;
    mockSetupArgs.cdcEnabled = true;

    sqlSetup = new SqlSetup(mockDatabase, mockSetupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 3);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateUsersStep);
    assertTrue(steps.get(2) instanceof CreateCdcUserStep);
  }
}
