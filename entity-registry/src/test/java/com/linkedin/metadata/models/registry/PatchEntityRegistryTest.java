package com.linkedin.metadata.models.registry;

import static org.testng.Assert.*;

import com.datahub.test.TestEntityProfile;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.testng.annotations.Test;

public class PatchEntityRegistryTest {

  @Test
  public void testEntityRegistryLoad() throws Exception, EntityRegistryException {
    PatchEntityRegistry patchEntityRegistry =
        new PatchEntityRegistry(
            TestConstants.BASE_DIRECTORY
                + "/"
                + TestConstants.TEST_REGISTRY
                + "/"
                + TestConstants.TEST_VERSION.toString(),
            TestConstants.TEST_REGISTRY,
            TestConstants.TEST_VERSION,
            null);

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
   *
   * @throws Exception
   * @throws EntityRegistryException
   */
  @Test
  public void testEntityRegistryWithKeyLoad() throws Exception, EntityRegistryException {
    Path pluginLocation =
        Paths.get(
            TestConstants.BASE_DIRECTORY
                + "/"
                + TestConstants.TEST_REGISTRY
                + "/"
                + TestConstants.TEST_VERSION.toString());

    DataSchemaFactory dataSchemaFactory = DataSchemaFactory.withCustomClasspath(pluginLocation);

    PatchEntityRegistry patchEntityRegistry =
        new PatchEntityRegistry(
            dataSchemaFactory,
            DataSchemaFactory.getClassLoader(pluginLocation).stream().toList(),
            Paths.get("src/test_plugins/mycompany-full-model/0.0.1/entity-registry.yaml"),
            TestConstants.TEST_REGISTRY,
            TestConstants.TEST_VERSION,
            null);

    Map<String, EntitySpec> entitySpecs = patchEntityRegistry.getEntitySpecs();
    assertEquals(entitySpecs.values().size(), 1);
    EntitySpec newThingSpec = patchEntityRegistry.getEntitySpec("newThing");
    assertNotNull(newThingSpec);
    assertNotNull(newThingSpec.getKeyAspectSpec());
    assertNotNull(newThingSpec.getAspectSpec(TestConstants.TEST_ASPECT_NAME));
  }

  private PatchEntityRegistry loadPluginRegistry(String yamlResource)
      throws Exception, EntityRegistryException {
    Path pluginLocation =
        Paths.get(
            TestConstants.BASE_DIRECTORY
                + "/"
                + TestConstants.TEST_REGISTRY
                + "/"
                + TestConstants.TEST_VERSION.toString());
    return new PatchEntityRegistry(
        DataSchemaFactory.withCustomClasspath(pluginLocation),
        DataSchemaFactory.getClassLoader(pluginLocation).stream().toList(),
        Paths.get("src/test/resources/" + yamlResource),
        TestConstants.TEST_REGISTRY,
        TestConstants.TEST_VERSION,
        null);
  }

  /** A plugin entity can list core aspects; the merged registry keeps the core aspect specs. */
  @Test
  public void testPluginEntityWithCoreAspect() throws Exception, EntityRegistryException {
    PatchEntityRegistry patchEntityRegistry =
        loadPluginRegistry("test-entity-registry-plugin-core-aspects.yml");

    EntitySpec newThingSpec = patchEntityRegistry.getEntitySpec("newThing");
    assertNotNull(newThingSpec.getAspectSpec("status"));
    assertNotNull(newThingSpec.getAspectSpec(TestConstants.TEST_ASPECT_NAME));
    assertFalse(patchEntityRegistry.getAspectSpecs().containsKey("status"));
    assertTrue(patchEntityRegistry.getAspectSpecs().containsKey(TestConstants.TEST_ASPECT_NAME));

    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-entity-registry.yml"));
    AspectSpec coreStatusSpec = baseRegistry.getAspectSpecs().get("status");
    MergedEntityRegistry mergedRegistry =
        new MergedEntityRegistry(baseRegistry).apply(patchEntityRegistry);
    assertNotNull(mergedRegistry.getEntitySpec("newThing").getAspectSpec("status"));
    assertSame(mergedRegistry.getAspectSpecs().get("status"), coreStatusSpec);
  }

  @Test
  public void testPluginEntityWithUnknownAspectFails() {
    assertThrows(
        IllegalArgumentException.class,
        () -> loadPluginRegistry("test-entity-registry-plugin-unknown-aspect.yml"));
  }
}
