package com.linkedin.metadata.models.registry;

import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.nio.file.Paths;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PatchEntityRegistryTest {

  @Test
  public void testEntityRegistryLoad() throws Exception, EntityRegistryException {
    PatchEntityRegistry patchEntityRegistry = new PatchEntityRegistry(
        TestConstants.BASE_DIRECTORY 
        + "/" 
        + TestConstants.TEST_REGISTRY 
        + "/" 
        + TestConstants.TEST_VERSION.toString(),
        TestConstants.TEST_REGISTRY, TestConstants.TEST_VERSION);

    Map<String, EntitySpec> entitySpecs = patchEntityRegistry.getEntitySpecs();
    assertEquals(entitySpecs.values().size(), 1);
    EntitySpec datasetSpec = patchEntityRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec);
    assertNull(datasetSpec.getKeyAspectSpec());
    assertNotNull(datasetSpec.getAspectSpec(TestConstants.TEST_ASPECT_NAME));

    Map<String, EventSpec> eventSpecs = patchEntityRegistry.getEventSpecs();
    for (EventSpec spec : eventSpecs.values()) {
      System.out.println(spec.getName());
    }
    assertEquals(eventSpecs.values().size(), 1);
    EventSpec dataQualityEvent = patchEntityRegistry.getEventSpec("dataQualityEvent");
    assertNotNull(dataQualityEvent);
  }

  /**
   * Validate that patch entity registries can have key aspects
   * @throws Exception
   * @throws EntityRegistryException
   */
  @Test
  public void testEntityRegistryWithKeyLoad() throws Exception, EntityRegistryException {
    DataSchemaFactory dataSchemaFactory = DataSchemaFactory.withCustomClasspath(
      Paths.get(TestConstants.BASE_DIRECTORY 
        + "/" 
        + TestConstants.TEST_REGISTRY 
        + "/" 
        + TestConstants.TEST_VERSION.toString()));

    PatchEntityRegistry patchEntityRegistry = new PatchEntityRegistry(
      dataSchemaFactory, Paths.get("src/test_plugins/mycompany-full-model/0.0.1/entity-registry.yaml"),
            TestConstants.TEST_REGISTRY, TestConstants.TEST_VERSION);

    Map<String, EntitySpec> entitySpecs = patchEntityRegistry.getEntitySpecs();
    assertEquals(entitySpecs.values().size(), 1);
    EntitySpec newThingSpec = patchEntityRegistry.getEntitySpec("newThing");
    assertNotNull(newThingSpec);
    assertNotNull(newThingSpec.getKeyAspectSpec());
    assertNotNull(newThingSpec.getAspectSpec(TestConstants.TEST_ASPECT_NAME));
  }
}
