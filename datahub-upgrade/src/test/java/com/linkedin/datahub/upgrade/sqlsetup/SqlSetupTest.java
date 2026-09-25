package com.linkedin.datahub.upgrade.sqlsetup;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.metadata.config.postgres.DatabaseType;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import io.ebean.Database;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SqlSetupTest {

  private static final String BANDS_JSON =
      "[{\"range\":[0,3],\"weight\":70},{\"range\":[4,6],\"weight\":20},{\"range\":[7,9],\"weight\":10}]";

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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);
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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);
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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);

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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);

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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);

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
            null, // postgresMetadataSchema
            false, // createSchemaVersionIndex
            null);

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);

    List<UpgradeStep> steps = sqlSetup.steps();
    assertEquals(steps.size(), 3);
    assertTrue(steps.get(0) instanceof CreateTablesStep);
    assertTrue(steps.get(1) instanceof CreateUsersStep);
    assertTrue(steps.get(2) instanceof CreateCdcUserStep);
  }

  @Test
  public void testBuildStepsIncludesPgQueueWhenPostgresAndEnabled() {
    PostgresSqlSetupProperties pg = PostgresSqlSetupProperties.disabled();
    PostgresSqlSetupProperties.PgQueue pq = pg.getPgQueue();
    pq.setEnabled(true);
    pq.setSchema("queue");
    pq.setTablePrefix("metadata_queue");
    PostgresSqlSetupProperties.PgQueue.TopicDefaults td = pq.getTopicDefaults();
    td.setPartitionCount(2);
    td.setVisibilityTimeoutSeconds(600);
    td.setPriorityBands(BANDS_JSON);
    td.setRetentionMaxAgeSeconds(0);
    td.setMaxRowsPerTopic(0L);
    td.setMaxTotalPayloadBytesPerTopic(0L);
    pq.getMaintenance().setCronEnabled(false);
    pq.getMaintenance().setIntervalSeconds(3600);
    pq.getMaintenance().setBatchDeleteLimit(5000);

    SqlSetupArgs setupArgs =
        new SqlSetupArgs(
            true,
            true,
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
            "datahub",
            "datahub",
            false,
            pg);

    sqlSetup = new SqlSetup(mockDatabase, setupArgs);
    List<String> stepIds =
        sqlSetup.steps().stream().map(UpgradeStep::id).collect(Collectors.toList());
    assertTrue(stepIds.stream().anyMatch(s -> s.equals("PgQueueSchemaStep")));
  }
}
