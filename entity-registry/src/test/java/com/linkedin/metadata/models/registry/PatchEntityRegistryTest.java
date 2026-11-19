package com.linkedin.metadata.models.registry;

import static org.testng.Assert.*;

import com.linkedin.metadata.models.DataSchemaFactory;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EventSpec;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.maven.artifact.versioning.ComparableVersion;
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

  private static final Path ENTITY_FIELDS_REGISTRY =
      Paths.get("src/test/resources/test-plugin-registry-entity-fields.yaml");

  private static PatchEntityRegistry loadFromCoreClasspath(Path registryFile)
      throws Exception, EntityRegistryException {
    return new PatchEntityRegistry(
        DataSchemaFactory.getInstance(),
        List.of(),
        registryFile,
        "test-plugin",
        new ComparableVersion("0.0.1"),
        null);
  }

  @Test
  public void testEntityFieldsFromYamlAreHonoured() throws Exception, EntityRegistryException {
    PatchEntityRegistry patchEntityRegistry = loadFromCoreClasspath(ENTITY_FIELDS_REGISTRY);

    EntitySpec dashboard = patchEntityRegistry.getEntitySpec("dashboard");
    assertEquals(dashboard.getSearchGroup(), "plugingroup");
    assertTrue(dashboard.isViewUnrestricted());

    // Fields left out of the YAML keep their defaults: no search group, view-restricted.
    EntitySpec dataset = patchEntityRegistry.getEntitySpec("dataset");
    assertNull(dataset.getSearchGroup());
    assertFalse(dataset.isViewUnrestricted());
  }

  @Test
  public void testMergedPluginEntityFields() throws Exception, EntityRegistryException {
    String baseYaml =
        """
        id: base-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: primary
            aspects:
              - status
        """;
    MergedEntityRegistry merged =
        new MergedEntityRegistry(
            new ConfigEntityRegistry(new ByteArrayInputStream(baseYaml.getBytes())));

    merged.apply(loadFromCoreClasspath(ENTITY_FIELDS_REGISTRY));

    EntitySpec dashboard = merged.getEntitySpec("dashboard");
    assertEquals(dashboard.getSearchGroup(), "plugingroup");
    assertTrue(dashboard.isViewUnrestricted());

    // An existing entity extended by the plugin keeps its base annotation.
    EntitySpec dataset = merged.getEntitySpec("dataset");
    assertEquals(dataset.getSearchGroup(), "primary");
    assertFalse(dataset.isViewUnrestricted());
  }

  @Test
  public void testPluginCannotLiftViewRestrictionOnExistingEntity()
      throws Exception, EntityRegistryException {
    String baseYaml =
        """
        id: base-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            aspects:
              - status
        """;
    MergedEntityRegistry merged =
        new MergedEntityRegistry(
            new ConfigEntityRegistry(new ByteArrayInputStream(baseYaml.getBytes())));

    merged.apply(
        loadFromCoreClasspath(
            Paths.get("src/test/resources/test-plugin-registry-view-unrestricted.yaml")));

    // The plugin asks for viewUnrestricted on an entity it only extends: the base value wins.
    assertFalse(merged.getEntitySpec("dataset").isViewUnrestricted());
    // A new entity that leaves the field out stays view-restricted.
    assertFalse(merged.getEntitySpec("chart").isViewUnrestricted());
  }

  @Test
  public void testMissingPluginClassFailsLoad() {
    EntityRegistryException thrown =
        expectThrows(
            EntityRegistryException.class,
            () ->
                loadFromCoreClasspath(
                    Paths.get(
                        "src/test/resources/broken_plugin_registries/broken-validator-model/0.0.1/entity-registry.yaml")));
    assertTrue(thrown.getMessage().contains("com.example.plugins.MissingValidator"));
  }
}
