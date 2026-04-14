package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Tests that the SystemEntityClient bean is correctly configured as a Java implementation during
 * blocking system updates.
 */
@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {"kafka.schemaRegistry.type=INTERNAL"},
    args = {"-u", "SystemUpdateBlocking"})
public class SystemUpdateConfigTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("systemEntityClient")
  private SystemEntityClient systemEntityClient;

  @Test
  public void testSystemEntityClientIsJavaImplementation() {
    assertNotNull(systemEntityClient, "SystemEntityClient bean should be created");
    assertTrue(
        systemEntityClient instanceof SystemJavaEntityClient,
        "SystemEntityClient should be Java implementation for blocking system updates, but was "
            + systemEntityClient.getClass().getName());
  }
}
