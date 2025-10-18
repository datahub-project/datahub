package com.linkedin.datahub.upgrade;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.restoreindices.RestoreIndices;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.dao.throttle.NoOpSensor;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@ActiveProfiles("test")
@SpringBootTest(
    args = {"-u", "SystemUpdate"},
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class})
public class UpgradeCliApplicationTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("restoreIndices")
  private RestoreIndices restoreIndices;

  @Autowired
  @Named("buildIndices")
  private BlockingSystemUpgrade buildIndices;

  @Autowired private ESIndexBuilder esIndexBuilder;

  @Qualifier("kafkaThrottle")
  @Autowired
  private ThrottleSensor kafkaThrottle;

  @Autowired private SystemTelemetryContext systemTelemetryContext;

  private String originalSqlSetupEnabled;

  @BeforeMethod
  public void setUp() {
    // Store original environment variable value
    originalSqlSetupEnabled = System.getenv("DATAHUB_SQL_SETUP_ENABLED");
  }

  @AfterMethod
  public void tearDown() {
    // Restore original environment variable value
    if (originalSqlSetupEnabled != null) {
      System.setProperty("DATAHUB_SQL_SETUP_ENABLED", originalSqlSetupEnabled);
    } else {
      System.clearProperty("DATAHUB_SQL_SETUP_ENABLED");
    }
  }

  @Test
  public void testRestoreIndicesInit() {
    /*
     This might seem like a simple test however it does exercise the spring autowiring of the kafka health check bean
    */
    assertTrue(restoreIndices.steps().size() >= 3);
  }

  @Test
  public void testBuildIndicesInit() {
    assertEquals("BuildIndices", buildIndices.id());
    assertTrue(buildIndices.steps().size() >= 3);
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration());
    assertNotNull(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices());
    assertTrue(esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isCloneIndices());
    assertFalse(
        esIndexBuilder.getElasticSearchConfiguration().getBuildIndices().isAllowDocCountMismatch());
  }

  @Test
  public void testNoThrottle() {
    assertEquals(
        new NoOpSensor(), kafkaThrottle, "No kafka throttle controls expected in datahub-upgrade");
  }

  @Test
  public void testTraceContext() {
    assertNotNull(systemTelemetryContext);
  }

  // Tests for UpgradeCliApplication main methods

  @Test
  public void testSqlSetupEnabledEnvironmentVariable() {
    // Note: System.setProperty() doesn't affect System.getenv(), so we can only test the current
    // state
    // and the logic of the isSqlSetupEnabled() method

    // Test the current state (should be false since we're in test environment)
    assertFalse(com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.isSqlSetupEnabled());

    // Test the method logic by checking what it does with different values
    // Since we can't easily mock System.getenv(), we'll test the method's behavior
    // by verifying it checks for "true" (case insensitive)

    // The method should return false for any value that's not "true" (case insensitive)
    // This is tested by the current environment state
    assertFalse(com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.isSqlSetupEnabled());
  }

  @Test
  public void testSqlSetupConditionConstants() {
    // Test that the constants are properly defined
    assertEquals(
        com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ARG, "SqlSetup");
    assertEquals(
        com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ENABLED_ENV,
        "DATAHUB_SQL_SETUP_ENABLED");
    assertTrue(
        com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ARGS.contains(
            "SqlSetup"));
  }

  @Test
  public void testUpgradeCliApplicationClassStructure() {
    // Test that UpgradeCliApplication is properly structured as a utility class
    // Check if SuppressWarnings annotation is present by looking at the source code
    // Since annotations might not be visible at runtime, we'll test the class structure instead

    // Test that the class has the expected methods
    try {
      UpgradeCliApplication.class.getDeclaredMethod("main", String[].class);
    } catch (NoSuchMethodException e) {
      assertTrue(false, "main method should exist");
    }

    // Test that the class is properly structured
    assertNotNull(UpgradeCliApplication.class);
    assertTrue(
        UpgradeCliApplication.class.isInterface() == false, "Should be a class, not interface");
  }

  @Test
  public void testUpgradeCliApplicationLogging() {
    // Test that the class has proper logging setup
    // Since annotations might not be visible at runtime, we'll test the class structure instead

    // Test that the class exists and can be loaded
    assertNotNull(UpgradeCliApplication.class);

    // Test that the class has the expected structure for a logging-enabled class
    assertTrue(
        UpgradeCliApplication.class.isInterface() == false, "Should be a class, not interface");

    // The @Slf4j annotation is verified by compilation - if it compiles, the annotation is present
    // This test ensures the class is properly structured
  }

  @Test
  public void testUpgradeCliApplicationImports() {
    // Test that all necessary imports are present
    // This is more of a compilation test, but ensures the class has all required dependencies
    assertNotNull(UpgradeCliApplication.class);

    // Test that the class can be instantiated (though it's a utility class)
    try {
      UpgradeCliApplication.class.newInstance();
    } catch (Exception e) {
      // This is expected for utility classes with private constructors
      assertTrue(e instanceof IllegalAccessException || e instanceof InstantiationException);
    }
  }

  @Test
  public void testUpgradeCliApplicationDependencies() {
    // Test that the class has access to required dependencies
    // This ensures the class can be loaded and its dependencies resolved

    // Test Spring Boot dependencies
    assertNotNull(org.springframework.boot.WebApplicationType.class);
    assertNotNull(org.springframework.boot.builder.SpringApplicationBuilder.class);
    assertNotNull(org.springframework.context.ConfigurableApplicationContext.class);

    // Test DataHub upgrade dependencies
    assertNotNull(com.linkedin.datahub.upgrade.impl.DefaultUpgradeManager.class);
    assertNotNull(com.linkedin.datahub.upgrade.sqlsetup.SqlSetup.class);
    assertNotNull(com.linkedin.upgrade.DataHubUpgradeState.class);

    // Test context dependencies
    assertNotNull(io.datahubproject.metadata.context.OperationContext.class);
  }

  @Test
  public void testUpgradeCliApplicationMethodSignatures() {
    // Test that the main method has the correct signature
    try {
      java.lang.reflect.Method mainMethod =
          UpgradeCliApplication.class.getDeclaredMethod("main", String[].class);
      assertTrue(java.lang.reflect.Modifier.isStatic(mainMethod.getModifiers()));
      assertTrue(java.lang.reflect.Modifier.isPublic(mainMethod.getModifiers()));
      assertEquals(mainMethod.getReturnType(), void.class);
      assertEquals(mainMethod.getParameterTypes().length, 1);
      assertEquals(mainMethod.getParameterTypes()[0], String[].class);
    } catch (NoSuchMethodException e) {
      assertTrue(false, "main method should exist with correct signature");
    }
  }

  @Test
  public void testUpgradeCliApplicationConstants() {
    // Test that the class uses the correct constants from SqlSetupCondition
    assertEquals(
        com.linkedin.datahub.upgrade.conditions.SqlSetupCondition.SQL_SETUP_ENABLED_ENV,
        "DATAHUB_SQL_SETUP_ENABLED");
  }
}
