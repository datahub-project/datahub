package com.linkedin.datahub.upgrade.sqlsetup.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeUtils;
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
    assertEquals(args.dbType, DatabaseType.MYSQL);
    assertEquals(args.host, "localhost");
    assertEquals(args.port, 3306);
    assertEquals(args.databaseName, "testdb");
    assertEquals(args.createUserUsername, "mysqluser");
    assertEquals(args.createUserPassword, "mysqlpass");
    assertEquals(args.iamAuthEnabled, false);
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
    assertEquals(args.dbType, DatabaseType.POSTGRES);
    assertEquals(args.host, "postgreshost");
    assertEquals(args.port, 5432);
    assertEquals(args.databaseName, "postgresdb");
    assertEquals(args.createUserUsername, "postgresuser");
    assertEquals(args.createUserPassword, "postgrespass");
    assertEquals(args.iamAuthEnabled, false);
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
    assertEquals(args.createTables, true);
    assertEquals(args.createDatabase, false);
    assertEquals(args.createUser, true);
    assertEquals(args.cdcEnabled, true);
    assertEquals(args.cdcUser, "custom_cdc");
    assertEquals(args.cdcPassword, "custom_cdc_pass");
    assertEquals(args.createUserIamRole, "arn:aws:iam::123456789012:role/datahub-role");
    assertEquals(args.iamAuthEnabled, true);
    assertEquals(args.createUserUsername, "testuser");
    assertEquals(args.createUserPassword, null); // No password for IAM auth
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
    assertEquals(args.createTables, true); // Default value
    assertEquals(args.createDatabase, true); // Default value
    assertEquals(args.createUser, false); // Default value
    assertEquals(args.cdcEnabled, false); // Default value
    assertEquals(args.cdcUser, "datahub_cdc"); // Default value
    assertEquals(args.cdcPassword, "datahub_cdc"); // Default value
    assertEquals(args.createUserIamRole, null); // Default value
  }

  @Test
  public void testCreateInstance() {
    SqlSetupArgs setupArgs = new SqlSetupArgs();
    setupArgs.dbType = DatabaseType.MYSQL;
    setupArgs.createTables = true;

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
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = true;
    args.createUser = true;
    args.createUserIamRole = null; // Missing IAM role
    args.createUserUsername = "testuser";

    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithIamButNoUsername() {
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = true;
    args.createUser = true;
    args.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    args.createUserUsername = null; // Missing createUserUsername

    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithTraditionalButNoUsername() {
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = false;
    args.createUser = true;
    args.createUserUsername = null; // Missing createUserUsername
    args.createUserPassword = "testpass";

    try {
      sqlSetupConfig.validateAuthenticationConfig(args);
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("CREATE_USER_USERNAME");
      throw e;
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateAuthenticationConfigWithTraditionalButNoPassword() {
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = false;
    args.createUser = true;
    args.createUserUsername = "testuser";
    args.createUserPassword = null; // Missing createUserPassword

    try {
      sqlSetupConfig.validateAuthenticationConfig(args);
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("CREATE_USER_PASSWORD");
      throw e;
    }
  }

  @Test
  public void testValidateAuthenticationConfigWithValidIam() {
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = true;
    args.createUserIamRole = "arn:aws:iam::123456789012:role/datahub-role";
    args.createUserUsername = "testuser";

    // Should not throw exception
    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test
  public void testValidateAuthenticationConfigWithValidTraditional() {
    SqlSetupArgs args = new SqlSetupArgs();
    args.iamAuthEnabled = false;
    args.createUserUsername = "testuser";
    args.createUserPassword = "testpass";

    // Should not throw exception
    sqlSetupConfig.validateAuthenticationConfig(args);
  }

  @Test
  public void testGetBooleanEnv() {
    // Test with "true" value
    System.setProperty("TEST_BOOL_TRUE", "true");
    boolean resultTrue = UpgradeUtils.getBoolean("TEST_BOOL_TRUE", false);
    assertTrue(resultTrue);

    // Test with "1" value
    System.setProperty("TEST_BOOL_ONE", "1");
    boolean resultOne = UpgradeUtils.getBoolean("TEST_BOOL_ONE", false);
    assertTrue(resultOne);

    // Test with "false" value
    System.setProperty("TEST_BOOL_FALSE", "false");
    boolean resultFalse = UpgradeUtils.getBoolean("TEST_BOOL_FALSE", true);
    assertTrue(!resultFalse);

    // Test with missing environment variable
    System.clearProperty("TEST_BOOL_MISSING");
    boolean resultMissing = UpgradeUtils.getBoolean("TEST_BOOL_MISSING", true);
    assertTrue(resultMissing);
  }

  @Test
  public void testGetStringEnv() throws Exception {
    System.setProperty("TEST_STRING_ENV", "test_value");
    String result = UpgradeUtils.getString("TEST_STRING_ENV", "default_value");
    assertEquals(result, "test_value");

    System.clearProperty("TEST_STRING_ENV");
    String defaultResult = UpgradeUtils.getString("TEST_STRING_ENV", "default_value");
    assertEquals(defaultResult, "default_value");
  }

  @Test
  public void testGetIntEnv() throws Exception {
    System.setProperty("TEST_INT_ENV", "42");
    int result = UpgradeUtils.getInt("TEST_INT_ENV", 10);
    assertEquals(result, 42);

    System.clearProperty("TEST_INT_ENV");
    int defaultResult = UpgradeUtils.getInt("TEST_INT_ENV", 10);
    assertEquals(defaultResult, 10);

    System.setProperty("TEST_INT_ENV_INVALID", "not_a_number");
    int invalidResult = UpgradeUtils.getInt("TEST_INT_ENV_INVALID", 10);
    assertEquals(invalidResult, 10);
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

    assertEquals(args.createUser, true);
    assertEquals(args.createUserUsername, "newuser");
    assertEquals(args.createUserPassword, "newpass");

    // Clean up
    System.clearProperty("CREATE_USER");
    System.clearProperty("CREATE_USER_USERNAME");
    System.clearProperty("CREATE_USER_PASSWORD");
    System.clearProperty("ebean.createUserUsername");
    System.clearProperty("ebean.createUserPassword");
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
    } finally {
      // Clean up
      System.clearProperty("CREATE_USER");
      System.clearProperty("ebean.createUserUsername");
      System.clearProperty("ebean.createUserPassword");
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

    assertEquals(args.createUser, false);
    assertEquals(args.createUserUsername, null); // Not used when CREATE_USER=false
    assertEquals(args.createUserPassword, null); // Not used when CREATE_USER=false

    // Clean up
    System.clearProperty("CREATE_USER");
    System.clearProperty("CREATE_USER_USERNAME");
    System.clearProperty("CREATE_USER_PASSWORD");
    System.clearProperty("ebean.createUserUsername");
    System.clearProperty("ebean.createUserPassword");
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
    SqlSetupArgs args1 = new SqlSetupArgs();
    args1.iamAuthEnabled = true;
    args1.createUser = true;
    args1.createUserIamRole = ""; // Empty role
    args1.createUserUsername = "testuser";

    try {
      sqlSetupConfig.validateAuthenticationConfig(args1);
      assertTrue(false, "Expected IllegalStateException for empty IAM role");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test IAM auth with whitespace-only role
    SqlSetupArgs args2 = new SqlSetupArgs();
    args2.iamAuthEnabled = true;
    args2.createUser = true;
    args2.createUserIamRole = "   "; // Whitespace-only role
    args2.createUserUsername = "testuser";

    try {
      sqlSetupConfig.validateAuthenticationConfig(args2);
      assertTrue(false, "Expected IllegalStateException for whitespace-only IAM role");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test traditional auth with empty createUserUsername
    SqlSetupArgs args3 = new SqlSetupArgs();
    args3.iamAuthEnabled = false;
    args3.createUser = true;
    args3.createUserUsername = ""; // Empty createUserUsername
    args3.createUserPassword = "testpass";

    try {
      sqlSetupConfig.validateAuthenticationConfig(args3);
      assertTrue(false, "Expected IllegalStateException for empty createUserUsername");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalStateException);
    }

    // Test traditional auth with whitespace-only createUserPassword
    SqlSetupArgs args4 = new SqlSetupArgs();
    args4.iamAuthEnabled = false;
    args4.createUser = true;
    args4.createUserUsername = "testuser";
    args4.createUserPassword = "   "; // Whitespace-only createUserPassword

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
    assertEquals(args.dbType, DatabaseType.POSTGRES);
    assertEquals(args.host, "postgreshost");
    assertEquals(args.port, 5432);
    assertEquals(args.databaseName, "complexdb");
    assertEquals(args.createUserUsername, "pguser");
    assertEquals(args.createUserPassword, null); // No password for IAM auth
    assertEquals(args.iamAuthEnabled, true);
  }

  @Test
  public void testCreateSetupArgsWithMinimalConfiguration() {
    // Set minimal required configuration
    sqlSetupConfig.setEbeanUrl("jdbc:mysql://localhost:3306/datahub");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.dbType, DatabaseType.MYSQL); // Default
    assertEquals(args.host, "localhost"); // Default
    assertEquals(args.port, 3306); // Default for MySQL
    assertEquals(args.databaseName, "datahub"); // Default
    assertNull(args.createUserUsername); // Empty default
    assertNull(args.createUserPassword); // Empty default
    assertFalse(args.iamAuthEnabled); // Default
  }

  @Test
  public void testCreateSetupArgsWithPostgresDefaults() {
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    // Set the ebeanUrl using package-private setter
    sqlSetupConfig.setEbeanUrl("jdbc:postgresql://localhost:5432/testdb");

    SqlSetupArgs args = sqlSetupConfig.createSetupArgs();

    assertNotNull(args);
    assertEquals(args.dbType, DatabaseType.POSTGRES);
    assertEquals(args.host, "localhost");
    assertEquals(args.port, 5432);
    assertEquals(args.databaseName, "testdb");
    assertEquals(args.port, 5432); // Should be 5432 for PostgreSQL when no explicit port
  }
}
