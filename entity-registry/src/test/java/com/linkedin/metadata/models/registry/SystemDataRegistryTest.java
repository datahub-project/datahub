package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityKey;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.EntitySpec;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SystemDataRegistryTest {

  private EntityRegistry entityRegistry;

  @BeforeTest
  public void init() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            SystemDataRegistryTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));
  }

  @Test
  public void testProductionSystemEntityRegistered() {
    EntitySpec systemState = entityRegistry.getEntitySpec("dataHubSystemState");
    assertTrue(systemState.isSystemEntity());
    assertFalse(systemState.isSystemEntityAllowRead());
  }

  @Test
  public void testSystemDataTestFixturesRegistered() throws Exception {
    EntityRegistry testRegistry =
        new ConfigEntityRegistry(
            TestEntityKey.class
                .getClassLoader()
                .getResourceAsStream("system-data-test-entity-registry.yml"));

    EntitySpec testEntity = testRegistry.getEntitySpec("testSystemData");
    assertTrue(testEntity.isSystemEntity());

    EntitySpec policyEligible = testRegistry.getEntitySpec("testSystemDataPolicyEligible");
    assertTrue(policyEligible.isSystemEntity());
    assertTrue(policyEligible.isSystemEntityAllowRead());
    assertTrue(policyEligible.isSystemEntityAllowExists());
  }
}
