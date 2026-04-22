package com.linkedin.datahub.upgrade.config;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.datahub.upgrade.UpgradeCliApplication;
import com.linkedin.datahub.upgrade.UpgradeCliApplicationTestConfiguration;
import com.linkedin.metadata.aspect.consistency.ConsistencyCheckRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyFixRegistry;
import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.systemmetadata.ESSystemMetadataDAO;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Tests that the ConsistencyService and its dependencies are correctly wired in the system-update
 * context.
 *
 * <p>This verifies that ESSystemMetadataDAO is properly exposed as a bean and can be injected into
 * ConsistencyService.
 */
@ActiveProfiles("test")
@SpringBootTest(
    classes = {UpgradeCliApplication.class, UpgradeCliApplicationTestConfiguration.class},
    properties = {"kafka.schemaRegistry.type=INTERNAL"},
    args = {"-u", "SystemUpdateNonBlocking"})
public class ConsistencyServiceBeanTest extends AbstractTestNGSpringContextTests {

  @Autowired
  @Named("esSystemMetadataDAO")
  private ESSystemMetadataDAO esSystemMetadataDAO;

  @Autowired
  @Named("consistencyService")
  private ConsistencyService consistencyService;

  @Autowired
  @Named("genericConsistencyCheckRegistry")
  private ConsistencyCheckRegistry checkRegistry;

  @Autowired
  @Named("genericConsistencyFixRegistry")
  private ConsistencyFixRegistry fixRegistry;

  @Test
  public void testESSystemMetadataDAOBeanExists() {
    assertNotNull(esSystemMetadataDAO, "ESSystemMetadataDAO bean should be created");
  }

  @Test
  public void testConsistencyServiceBeanExists() {
    assertNotNull(consistencyService, "ConsistencyService bean should be created");
  }

  @Test
  public void testConsistencyCheckRegistryBeanExists() {
    assertNotNull(checkRegistry, "ConsistencyCheckRegistry bean should be created");
  }

  @Test
  public void testConsistencyFixRegistryBeanExists() {
    assertNotNull(fixRegistry, "ConsistencyFixRegistry bean should be created");
  }

  @Test
  public void testCheckRegistryHasChecks() {
    // Verify that checks have been registered (from component scan)
    assertNotNull(
        checkRegistry.getEntityTypes(),
        "Check registry should have entity types after component scan");
  }

  @Test
  public void testFixRegistryHasFixes() {
    // Verify that fixes have been registered (from component scan)
    // At minimum, BatchItemsFix should be registered handling multiple fix types
    assertTrue(fixRegistry.size() > 0, "Fix registry should have fixes after component scan");
  }
}
