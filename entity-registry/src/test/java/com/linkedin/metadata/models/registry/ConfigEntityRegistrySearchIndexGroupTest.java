package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.linkedin.metadata.models.EntitySpec;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class ConfigEntityRegistrySearchIndexGroupTest {

  @BeforeTest
  public void disableAssert() {
    com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(
            com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
                .getName(),
            false);
  }

  @Test
  public void testFixtureEntitiesHaveUnsetSearchGroup() throws IOException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            ConfigEntityRegistrySearchIndexGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    assertEquals(entitySpecs.size(), 8);

    for (EntitySpec spec : entitySpecs.values()) {
      assertEquals(spec.getSearchGroup(), null, spec.getName());
    }

    EntitySpec testEntitySpec = configEntityRegistry.getEntitySpec("testEntity");
    assertTrue(testEntitySpec.isViewUnrestricted());
    assertFalse(configEntityRegistry.getEntitySpec("dataset").isViewUnrestricted());
  }

  @Test
  public void testYamlSearchGroupIsParsedWhenPresent() {
    String yaml =
        """
        id: grouped-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: primary
            aspects: []
          - name: dataProcessInstance
            keyAspect: dataProcessInstanceKey
            searchGroup: timeseries
            aspects: []
          - name: query
            keyAspect: queryKey
            searchGroup: query
            aspects: []
          - name: schemaField
            keyAspect: schemaFieldKey
            searchGroup: schemaField
            aspects: []
          - name: dataHubPolicy
            keyAspect: dataHubPolicyKey
            searchGroup: default
            aspects: []
          - name: testEntity
            keyAspect: testEntityKey
            aspects: []
        """;
    ConfigEntityRegistry registry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(yaml.getBytes()));

    assertEquals(registry.getEntitySpec("dataset").getSearchGroup(), "primary");
    assertEquals(registry.getEntitySpec("dataProcessInstance").getSearchGroup(), "timeseries");
    assertEquals(registry.getEntitySpec("query").getSearchGroup(), "query");
    assertEquals(registry.getEntitySpec("schemaField").getSearchGroup(), "schemaField");
    assertEquals(registry.getEntitySpec("dataHubPolicy").getSearchGroup(), "default");
    assertEquals(registry.getEntitySpec("testEntity").getSearchGroup(), null);

    assertEquals(registry.getEntitySpecsBySearchGroup("primary").size(), 1);
    assertEquals(registry.getSearchGroups().size(), 5);
  }

  @Test
  public void testEntityRegistryIdentifier() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));
    assertEquals(configEntityRegistry.getIdentifier(), "test-search-index-group-registry");
  }

  @Test
  public void testEntityRegistryAspectSpecs() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Test that aspect specs are properly loaded
    EntitySpec datasetSpec = configEntityRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec.getAspectSpec("datasetKey"));
    assertNotNull(datasetSpec.getAspectSpec("datasetProperties"));
    assertNotNull(datasetSpec.getAspectSpec("schemaMetadata"));
    assertNotNull(datasetSpec.getAspectSpec("status"));
    assertEquals(datasetSpec.getAspectSpecs().size(), 4);

    EntitySpec chartSpec = configEntityRegistry.getEntitySpec("chart");
    assertNotNull(chartSpec.getAspectSpec("chartKey"));
    assertNotNull(chartSpec.getAspectSpec("chartInfo"));
    assertNotNull(chartSpec.getAspectSpec("status"));
    assertEquals(chartSpec.getAspectSpecs().size(), 3);
  }

  @Test
  public void testEntityRegistryKeyAspects() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Test that key aspects are properly loaded
    EntitySpec datasetSpec = configEntityRegistry.getEntitySpec("dataset");
    assertEquals(datasetSpec.getKeyAspectName(), "datasetKey");
    assertNotNull(datasetSpec.getKeyAspectSpec());

    EntitySpec chartSpec = configEntityRegistry.getEntitySpec("chart");
    assertEquals(chartSpec.getKeyAspectName(), "chartKey");
    assertNotNull(chartSpec.getKeyAspectSpec());

    EntitySpec dataProcessInstanceSpec = configEntityRegistry.getEntitySpec("dataProcessInstance");
    assertEquals(dataProcessInstanceSpec.getKeyAspectName(), "dataProcessInstanceKey");
    assertNotNull(dataProcessInstanceSpec.getKeyAspectSpec());
  }

  @Test
  public void testEntityRegistryCategories() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            TestEntityProfile.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Test that categories are properly loaded
    EntitySpec datasetSpec = configEntityRegistry.getEntitySpec("dataset");
    assertEquals(datasetSpec.getEntityAnnotation().getClass().getSimpleName(), "EntityAnnotation");

    EntitySpec dataHubPolicySpec = configEntityRegistry.getEntitySpec("dataHubPolicy");
    assertEquals(
        dataHubPolicySpec.getEntityAnnotation().getClass().getSimpleName(), "EntityAnnotation");
  }

  @Test
  public void testGetEntitySpecsBySearchGroup() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            ConfigEntityRegistrySearchIndexGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    assertEquals(configEntityRegistry.getEntitySpecsBySearchGroup("primary").size(), 0);
    assertEquals(configEntityRegistry.getEntitySpecsBySearchGroup("default").size(), 0);
    assertEquals(configEntityRegistry.getEntitySpecsBySearchGroup("nonExistent").size(), 0);
  }

  @Test
  public void testGetSearchGroups() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            ConfigEntityRegistrySearchIndexGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    assertTrue(configEntityRegistry.getSearchGroups().isEmpty());
  }
}
