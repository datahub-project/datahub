package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.gms.factory.test.TestEngineFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * ACRYL-ONLY: Tests that TestEngineFactory is excluded from the general upgrade configuration.
 *
 * <p>This test validates that TestEngineFactory is EXCLUDED during SystemUpdateBlocking and
 * SystemUpdateNonBlocking upgrades. The inclusion test for EvaluateTests is in {@link
 * com.linkedin.datahub.upgrade.test.EvaluateTestsStepTest#testTestEngineFactoryIsIncluded()}.
 *
 * <p>Background: The TestEngine starts a background refresh thread when instantiated with metadata
 * tests enabled. This was causing unnecessary resource usage during system-update runs. The fix
 * excludes TestEngineFactory from GeneralUpgradeConfiguration's component scan, while
 * EvaluateTestsConfig explicitly imports it when the -u EvaluateTests argument is used.
 *
 * @see GeneralUpgradeConfiguration
 * @see EvaluateTestsConfig
 * @see <a href="https://github.com/datahub-project/datahub/commit/03d0daa9f2">Original fix
 *     commit</a>
 */
public final class GeneralUpgradeConfigurationTest {

  private GeneralUpgradeConfigurationTest() {
    // Container class for nested test classes
  }

  /** Verifies that TestEngineFactory is excluded from the component scan. */
  private static void assertTestEngineFactoryIsExcluded(ApplicationContext applicationContext) {
    boolean hasTestEngineFactory = false;
    try {
      applicationContext.getBean(TestEngineFactory.class);
      hasTestEngineFactory = true;
    } catch (NoSuchBeanDefinitionException e) {
      // Expected - TestEngineFactory should not be loaded
    }
    assertFalse(
        hasTestEngineFactory,
        "TestEngineFactory should be excluded from GeneralUpgradeConfiguration component scan. "
            + "If this test fails, it means TestEngine's background refresh thread will run "
            + "during system-update, causing unnecessary resource usage.");
  }

  /** Tests that TestEngineFactory is excluded during non-blocking system upgrades. */
  @ActiveProfiles("test")
  @SpringBootTest(
      classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
      properties = {"kafka.schemaRegistry.type=INTERNAL"},
      args = {"-u", "SystemUpdateNonBlocking"})
  public static class NonBlockingUpgradeTest extends AbstractTestNGSpringContextTests {

    @Autowired private ApplicationContext applicationContext;

    @Test
    public void testApplicationContextLoadsSuccessfully() {
      assertNotNull(applicationContext, "ApplicationContext should be loaded");
      assertTrue(
          applicationContext.containsBean("systemOperationContext"),
          "systemOperationContext bean should be present");
    }

    @Test
    public void testTestEngineFactoryIsExcluded() {
      assertTestEngineFactoryIsExcluded(applicationContext);
    }
  }

  /** Tests that TestEngineFactory is excluded during blocking system upgrades. */
  @ActiveProfiles("test")
  @SpringBootTest(
      classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
      properties = {"kafka.schemaRegistry.type=INTERNAL"},
      args = {"-u", "SystemUpdateBlocking"})
  public static class BlockingUpgradeTest extends AbstractTestNGSpringContextTests {

    @Autowired private ApplicationContext applicationContext;

    @Test
    public void testApplicationContextLoadsSuccessfully() {
      assertNotNull(applicationContext, "ApplicationContext should be loaded");
      assertTrue(
          applicationContext.containsBean("systemOperationContext"),
          "systemOperationContext bean should be present");
    }

    @Test
    public void testTestEngineFactoryIsExcluded() {
      assertTestEngineFactoryIsExcluded(applicationContext);
    }
  }
}
