package com.linkedin.metadata.policy;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityKey;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.models.SystemDataEntityFixture;
import com.linkedin.test.metadata.models.SystemDataEntityFixture.EntityFixture;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SystemDataPolicyIndexTest {

  private EntityRegistry testRegistry;

  @BeforeClass
  public void init() throws Exception {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    testRegistry =
        new ConfigEntityRegistry(
            TestEntityKey.class
                .getClassLoader()
                .getResourceAsStream("system-data-test-entity-registry.yml"));
  }

  @AfterMethod
  public void clearCache() {
    SystemDataPolicyIndex.clearCache();
  }

  @Test
  public void testEntityExistsEligibleImpliedByAllowRead() {
    EntityFixture fixture = SystemDataEntityFixture.systemEntity("system", true, false);
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("system", fixture.getEntitySpec()));
    Mockito.when(registry.getEntitySpec("system")).thenReturn(fixture.getEntitySpec());

    SystemDataPolicyIndex policy = SystemDataPolicy.index(registry);

    assertTrue(policy.isEntityReadEligible("system"));
    assertTrue(policy.isEntityExistsEligible("system"));
    assertTrue(policy.isExistsEligible("system", fixture.getEntitySpec().getKeyAspectName()));
  }

  @Test
  public void testHiddenSystemEntityCachedDecisions() {
    SystemDataPolicyIndex policy = SystemDataPolicy.index(testRegistry);

    assertTrue(policy.isKnownEntity("testSystemData"));
    assertTrue(policy.requiresSystemActorWrite("testSystemData"));
    assertFalse(policy.isEntityReadEligible("testSystemData"));
    assertFalse(policy.isEntityExistsEligible("testSystemData"));
    assertFalse(policy.isReadEligible("testSystemData", "testSystemDataInfo"));
    assertFalse(policy.isExistsEligible("testSystemData", "testSystemDataKey"));
    assertFalse(policy.isExistsEligible("testSystemData", "testSystemDataInfo"));
  }

  @Test
  public void testPolicyEligibleSystemEntityCachedDecisions() {
    SystemDataPolicyIndex policy = SystemDataPolicy.index(testRegistry);

    assertTrue(policy.isEntityReadEligible("testSystemDataPolicyEligible"));
    assertTrue(policy.isEntityExistsEligible("testSystemDataPolicyEligible"));
    assertTrue(
        policy.isReadEligible("testSystemDataPolicyEligible", "testSystemDataPolicyEligibleInfo"));
    assertTrue(
        policy.isExistsEligible("testSystemDataPolicyEligible", "testSystemDataPolicyEligibleKey"));
    assertFalse(
        policy.isReadEligible("testSystemDataPolicyEligible", "testSystemDataHiddenAspect"));
    assertFalse(
        policy.isExistsEligible("testSystemDataPolicyEligible", "testSystemDataHiddenAspect"));
  }

  @Test
  public void testNormalEntityFastPath() {
    SystemDataPolicyIndex policy = SystemDataPolicy.index(testRegistry);

    assertFalse(policy.requiresSystemActorWrite("dataset"));
    assertTrue(policy.isReadEligible("dataset", "datasetProperties"));
  }

  @Test
  public void testIndexIsCachedPerRegistry() {
    SystemDataPolicyIndex first = SystemDataPolicy.index(testRegistry);
    SystemDataPolicyIndex second = SystemDataPolicy.index(testRegistry);
    assertTrue(first == second);
  }
}
