package com.linkedin.datahub.upgrade.sqlsetup.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.sqlsetup.DatabaseType;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetup;
import com.linkedin.datahub.upgrade.sqlsetup.SqlSetupArgs;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SqlSetupConfigTest {

  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private Database mockDatabase;

  private SqlSetupConfig sqlSetupConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    sqlSetupConfig = new SqlSetupConfig();

    // Clear all system properties that might interfere between tests
    clearSystemProperties();
  }

  @AfterClass
  public static void tearDownClass() {
    // Clean up system properties after all tests in this class have run
    clearSystemProperties();
  }

  private static void clearSystemProperties() {
    // Clear all system properties used in tests
    System.clearProperty("CREATE_USER");
    System.clearProperty("CREATE_USER_USERNAME");
    System.clearProperty("CREATE_USER_PASSWORD");
    System.clearProperty("CREATE_USER_IAM_ROLE");
    System.clearProperty("IAM_ROLE");
    System.clearProperty("DB_TYPE");
    System.clearProperty("CREATE_TABLES");
    System.clearProperty("CREATE_DB");
    System.clearProperty("CDC_MCL_PROCESSING_ENABLED");
    System.clearProperty("CDC_USER");
    System.clearProperty("CDC_PASSWORD");
  }

  @Test
  public void testMetricUtils() {
    MetricUtils metricUtils = sqlSetupConfig.metricUtils();

    assertNotNull(metricUtils);
    assertTrue(metricUtils instanceof MetricUtils);
  }

  @Test
  public void testOperationContext() {
    OperationContext operationContext = sqlSetupConfig.operationContext(mockEntityRegistry);

    assertNotNull(operationContext);
    assertTrue(operationContext instanceof OperationContext);
  }

  @Test
  public void testCreateSetupArgsWithMysql() {
    // Set up environment variables for MySQL
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/testdb");
    System.setProperty("CREATE_USER", "true");
    System.setProperty("CREATE_USER_USERNAME", "mysqluser");
    System.setProperty("CREATE_USER_PASSWORD", "mysqlpass");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.getDbType(), DatabaseType.MYSQL);
    assertEquals(args.getHost(), "localhost");
    assertEquals(args.getPort(), 3306);
    assertEquals(args.getDatabaseName(), "testdb");
    assertEquals(args.getCreateUserUsername(), "mysqluser");
    assertEquals(args.getCreateUserPassword(), "mysqlpass");
    assertEquals(args.isIamAuthEnabled(), false);
  }

  @Test
  public void testCreateSetupArgsWithPostgres() {
    // Set up environment variables for PostgreSQL
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://postgreshost:5432/postgresdb");
    System.setProperty("CREATE_USER", "true");
    System.setProperty("CREATE_USER_USERNAME", "postgresuser");
    System.setProperty("CREATE_USER_PASSWORD", "postgrespass");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.getDbType(), DatabaseType.POSTGRES);
    assertEquals(args.getHost(), "postgreshost");
    assertEquals(args.getPort(), 5432);
    assertEquals(args.getDatabaseName(), "postgresdb");
    assertEquals(args.getCreateUserUsername(), "postgresuser");
    assertEquals(args.getCreateUserPassword(), "postgrespass");
    assertEquals(args.isIamAuthEnabled(), false);
  }

  @Test
  public void testCreateSetupArgsWithEnvironmentVariables() {
    // Set up environment variables
    System.setProperty("CREATE_TABLES", "true");
    System.setProperty("CREATE_DB", "false");
    System.setProperty("CREATE_USER", "true");
    System.setProperty("CREATE_USER_USERNAME", "testuser");
    System.setProperty("CDC_MCL_PROCESSING_ENABLED", "true");
    System.setProperty("CDC_USER", "custom_cdc");
    System.setProperty("CDC_PASSWORD", "custom_cdc_pass");
    System.setProperty("IAM_ROLE", "arn:aws:iam::123456789012:role/datahub-role");
    // Ensure ebean.url is set (required)
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.isCreateTables(), true);
    assertEquals(args.isCreateDatabase(), false);
    assertEquals(args.isCreateUser(), true);
    assertEquals(args.isCdcEnabled(), true);
    assertEquals(args.getCdcUser(), "custom_cdc");
    assertEquals(args.getCdcPassword(), "custom_cdc_pass");
    assertEquals(args.getCreateUserIamRole(), "arn:aws:iam::123456789012:role/datahub-role");
    assertEquals(args.isIamAuthEnabled(), true);
    assertEquals(args.getCreateUserUsername(), "testuser");
    assertEquals(args.getCreateUserPassword(), null); // No password for IAM auth
  }

  @Test
  public void testCreateSetupArgsWithDefaultValues() {
    // Clear environment variables to test defaults
    System.clearProperty("CREATE_TABLES");
    System.clearProperty("CREATE_DB");
    System.clearProperty("CREATE_USER");
    System.clearProperty("CDC_MCL_PROCESSING_ENABLED");
    System.clearProperty("CDC_USER");
    System.clearProperty("CDC_PASSWORD");
    System.clearProperty("IAM_ROLE");

    // Ensure ebean.url is set (required)
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.isCreateTables(), true); // Default value
    assertEquals(args.isCreateDatabase(), true); // Default value
    assertEquals(args.isCreateUser(), false); // Default value
    assertEquals(args.isCdcEnabled(), false); // Default value
    assertEquals(args.getCdcUser(), "datahub_cdc"); // Default value
    assertEquals(args.getCdcPassword(), "datahub_cdc"); // Default value
    assertEquals(args.getCreateUserIamRole(), null); // Default value
  }

  @Test
  public void testCreateInstance() {
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
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    SqlSetup sqlSetup = sqlSetupConfig.createInstance(mockDatabase, setupArgs);

    assertNotNull(sqlSetup);
    assertTrue(sqlSetup instanceof SqlSetup);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testCreateNotImplInstance() {
    sqlSetupConfig.createNotImplInstance();
  }

  @Test
  public void testDetectDatabaseTypeFromExplicitDbType() {
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    DatabaseType dbType = sqlSetupConfig.detectDatabaseType();
    assertEquals(dbType, DatabaseType.POSTGRES);
  }

  @Test
  public void testDetectDatabaseTypeFromPostgresUrl() {
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    DatabaseType detectedType = sqlSetupConfig.detectDatabaseType();
    assertEquals(detectedType, DatabaseType.POSTGRES);
  }

  @Test
  public void testDetectDatabaseTypeFromMysqlUrl() {
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/testdb");

    DatabaseType mysqlType = sqlSetupConfig.detectDatabaseType();
    assertEquals(mysqlType, DatabaseType.MYSQL);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithIamButNoRole() {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            true, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            null, // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            null // createUserIamRole - Missing IAM role
            );

    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithIamButNoUsername() {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            true, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            null, // createUserUsername - Missing createUserUsername
            null, // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            "arn:aws:iam::123456789012:role/datahub-role" // createUserIamRole
            );

    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithTraditionalButNoUsername() {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            null, // createUserUsername - Missing createUserUsername
            "testpass", // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args);
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("CREATE_USER_USERNAME");
      throw e;
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithTraditionalButNoPassword() {
    SqlSetupArgs args =
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
            null, // createUserPassword - Missing createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args);
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("CREATE_USER_PASSWORD");
      throw e;
    }
  }

  @Test
  public void testValidateAuthenticationConfigWithValidIam() {
    SqlSetupArgs args =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            true, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            null, // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            "arn:aws:iam::123456789012:role/datahub-role" // createUserIamRole
            );

    // Should not throw exception
    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test
  public void testValidateAuthenticationConfigWithValidTraditional() {
    SqlSetupArgs args =
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
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    // Should not throw exception
    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test
  public void testCreateUserEnvironmentVariables() throws Exception {
    // Test that CREATE_USER_USERNAME and CREATE_USER_PASSWORD are used when CREATE_USER is enabled
    System.setProperty("CREATE_USER", "true");
    System.setProperty("CREATE_USER_USERNAME", "newuser");
    System.setProperty("CREATE_USER_PASSWORD", "newpass");
    System.setProperty("ebean.createUserUsername", "ebeanuser");
    System.setProperty("ebean.createUserPassword", "ebeanpass");
    // Ensure ebean.url is set (required)
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertEquals(args.isCreateUser(), true);
    assertEquals(args.getCreateUserUsername(), "newuser");
    assertEquals(args.getCreateUserPassword(), "newpass");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testCreateUserEnvironmentVariablesFallback() throws Exception {
    // Test that validation fails when CREATE_USER is enabled but env vars are not set
    System.setProperty("CREATE_USER", "true");

    // Ensure ebean.url is set (required)
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    try {
      SqlSetupArgs args = sqlSetupConfig.createSetupArgs();
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("CREATE_USER_USERNAME");
      throw e;
    }
  }

  @Test
  public void testCreateUserDisabledUsesEbeanCredentials() throws Exception {
    // Test that when CREATE_USER is disabled, createUserUsername/createUserPassword fields are null
    System.setProperty("CREATE_USER", "false");
    System.setProperty("CREATE_USER_USERNAME", "newuser");
    System.setProperty("CREATE_USER_PASSWORD", "newpass");
    System.setProperty("ebean.createUserUsername", "ebeanuser");
    System.setProperty("ebean.createUserPassword", "ebeanpass");
    // Ensure ebean.url is set (required)
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertEquals(args.isCreateUser(), false);
    assertEquals(args.getCreateUserUsername(), null); // Not used when CREATE_USER=false
    assertEquals(args.getCreateUserPassword(), null); // Not used when CREATE_USER=false
  }

  @Test
  public void testDetectDatabaseTypeEdgeCases() throws Exception {
    // Test detection from URL with different cases
    sqlSetupConfig.setEbeanUrl("jdbc:PostgreSQL://localhost:5432/testdb");
    DatabaseType postgresResult = sqlSetupConfig.detectDatabaseType();
    assertEquals(postgresResult, DatabaseType.POSTGRES);

    sqlSetupConfig.setEbeanUrl("jdbc:MySQL://localhost:3306/testdb");
    DatabaseType mysqlResult = sqlSetupConfig.detectDatabaseType();
    assertEquals(mysqlResult, DatabaseType.MYSQL);

    // Test MariaDB detection
    sqlSetupConfig.setEbeanUrl("jdbc:mariadb://localhost:3306/testdb");
    DatabaseType mariadbResult = sqlSetupConfig.detectDatabaseType();
    assertEquals(mariadbResult, DatabaseType.MYSQL); // MariaDB maps to mysql
  }

  @Test
  public void testValidateAuthenticationConfigEdgeCases() throws Exception {
    // Test IAM auth with empty role
    SqlSetupArgs args1 =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            true, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            null, // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            "" // createUserIamRole - Empty role
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args1);
      assertTrue(false, "Expected IllegalStateException for empty IAM role");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test IAM auth with whitespace-only role
    SqlSetupArgs args2 =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            true, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "testuser", // createUserUsername
            null, // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            "   " // createUserIamRole - Whitespace-only role
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args2);
      assertTrue(false, "Expected IllegalStateException for whitespace-only IAM role");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test traditional auth with empty createUserUsername
    SqlSetupArgs args3 =
        new SqlSetupArgs(
            true, // createTables
            true, // createDatabase
            true, // createUser
            false, // iamAuthEnabled
            DatabaseType.MYSQL, // dbType
            false, // cdcEnabled
            "datahub_cdc", // cdcUser
            "datahub_cdc", // cdcPassword
            "", // createUserUsername - Empty createUserUsername
            "testpass", // createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args3);
      assertTrue(false, "Expected IllegalStateException for empty createUserUsername");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test traditional auth with whitespace-only createUserPassword
    SqlSetupArgs args4 =
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
            "   ", // createUserPassword - Whitespace-only createUserPassword
            "localhost", // host
            0, // port
            "datahub", // databaseName
            null // createUserIamRole
            );

    try {
      sqlSetupConfig.validateAuthenticationConfig(args4);
      assertTrue(false, "Expected IllegalStateException for whitespace-only createUserPassword");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }
  }

  @Test
  public void testCreateSetupArgsWithComplexUrl() {
    String complexUrl =
        "jdbc:postgresql://user:pass@postgreshost:5432/complexdb?sslmode=require&application_name=datahub";
    sqlSetupConfig.setEbeanUrl(complexUrl);
    System.setProperty("CREATE_USER", "true");
    System.setProperty("CREATE_USER_USERNAME", "pguser");
    System.setProperty("CREATE_USER_PASSWORD", "pgpass");
    System.setProperty("IAM_ROLE", "my/role");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.getDbType(), DatabaseType.POSTGRES);
    assertEquals(args.getHost(), "postgreshost");
    assertEquals(args.getPort(), 5432);
    assertEquals(args.getDatabaseName(), "complexdb");
    assertEquals(args.getCreateUserUsername(), "pguser");
    assertEquals(args.getCreateUserPassword(), null); // No password for IAM auth
    assertEquals(args.isIamAuthEnabled(), true);
  }

  @Test
  public void testCreateSetupArgsWithMinimalConfiguration() {
    // Set minimal required configuration
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.getDbType(), DatabaseType.MYSQL); // Default
    assertEquals(args.getHost(), "localhost"); // Default
    assertEquals(args.getPort(), 3306); // Default for MySQL
    assertEquals(args.getDatabaseName(), "datahub"); // Default
    assertNull(args.getCreateUserUsername()); // Empty default
    assertNull(args.getCreateUserPassword()); // Empty default
    assertFalse(args.isIamAuthEnabled()); // Default
  }

  @Test
  public void testCreateSetupArgsWithPostgresDefaults() {
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    // Set the ebeanUrl using package-private setter
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.getDbType(), DatabaseType.POSTGRES);
    assertEquals(args.getHost(), "localhost");
    assertEquals(args.getPort(), 5432);
    assertEquals(args.getDatabaseName(), "testdb");
    assertEquals(args.getPort(), 5432); // Should be 5432 for PostgreSQL when no explicit port
  }
}
