package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.test.TestEntityProfile;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
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
  public void testEntityRegistryWithSearchGroups() throws IOException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            ConfigEntityRegistrySearchIndexGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    assertEquals(entitySpecs.size(), 8);

    // Test search group entities
    EntitySpec datasetSpec = configEntityRegistry.getEntitySpec("dataset");
    assertEquals(datasetSpec.getName(), "dataset");
    assertEquals(datasetSpec.getSearchGroup(), "primary");

    EntitySpec chartSpec = configEntityRegistry.getEntitySpec("chart");
    assertEquals(chartSpec.getName(), "chart");
    assertEquals(chartSpec.getSearchGroup(), "primary");

    // Test timeseries group entities
    EntitySpec dataProcessInstanceSpec = configEntityRegistry.getEntitySpec("dataProcessInstance");
    assertEquals(dataProcessInstanceSpec.getName(), "dataProcessInstance");
    assertEquals(dataProcessInstanceSpec.getSearchGroup(), "timeseries");

    EntitySpec dataHubExecutionRequestSpec =
        configEntityRegistry.getEntitySpec("dataHubExecutionRequest");
    assertEquals(dataHubExecutionRequestSpec.getName(), "dataHubExecutionRequest");
    assertEquals(dataHubExecutionRequestSpec.getSearchGroup(), "timeseries");

    // Test query group entity
    EntitySpec querySpec = configEntityRegistry.getEntitySpec("query");
    assertEquals(querySpec.getName(), "query");
    assertEquals(querySpec.getSearchGroup(), "query");

    // Test schemaField group entity
    EntitySpec schemaFieldSpec = configEntityRegistry.getEntitySpec("schemaField");
    assertEquals(schemaFieldSpec.getName(), "schemaField");
    assertEquals(schemaFieldSpec.getSearchGroup(), "schemaField");

    // Test default group entity
    EntitySpec dataHubPolicySpec = configEntityRegistry.getEntitySpec("dataHubPolicy");
    assertEquals(dataHubPolicySpec.getName(), "dataHubPolicy");
    assertEquals(dataHubPolicySpec.getSearchGroup(), "default");

    // Test entity without searchGroup (should default to "default")
    EntitySpec testEntitySpec = configEntityRegistry.getEntitySpec("testEntity");
    assertEquals(testEntitySpec.getName(), "testEntity");
    assertEquals(testEntitySpec.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
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

    // Test primary group
    Map<String, EntitySpec> primaryGroup =
        configEntityRegistry.getEntitySpecsBySearchGroup("primary");
    assertEquals(primaryGroup.size(), 2);
    assertTrue(primaryGroup.containsKey("dataset"));
    assertTrue(primaryGroup.containsKey("chart"));

    // Test timeseries group
    Map<String, EntitySpec> timeseriesGroup =
        configEntityRegistry.getEntitySpecsBySearchGroup("timeseries");
    assertEquals(timeseriesGroup.size(), 2);
    assertTrue(timeseriesGroup.containsKey("dataprocessinstance"));
    assertTrue(timeseriesGroup.containsKey("datahubexecutionrequest"));

    // Test query group
    Map<String, EntitySpec> queryGroup = configEntityRegistry.getEntitySpecsBySearchGroup("query");
    assertEquals(queryGroup.size(), 1);
    assertTrue(queryGroup.containsKey("query"));

    // Test schemaField group
    Map<String, EntitySpec> schemaFieldGroup =
        configEntityRegistry.getEntitySpecsBySearchGroup("schemaField");
    assertEquals(schemaFieldGroup.size(), 1);
    assertTrue(schemaFieldGroup.containsKey("schemafield"));

    // Test default group
    Map<String, EntitySpec> defaultGroup =
        configEntityRegistry.getEntitySpecsBySearchGroup("default");
    assertEquals(defaultGroup.size(), 2);
    assertTrue(defaultGroup.containsKey("datahubpolicy"));
    assertTrue(defaultGroup.containsKey("testentity"));

    // Test non-existent group
    Map<String, EntitySpec> nonExistentGroup =
        configEntityRegistry.getEntitySpecsBySearchGroup("nonExistent");
    assertEquals(nonExistentGroup.size(), 0);
  }

  @Test
  public void testGetSearchGroups() throws FileNotFoundException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            ConfigEntityRegistrySearchIndexGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    var searchGroups = configEntityRegistry.getSearchGroups();
    assertEquals(searchGroups.size(), 5);
    assertTrue(searchGroups.contains("primary"));
    assertTrue(searchGroups.contains("timeseries"));
    assertTrue(searchGroups.contains("query"));
    assertTrue(searchGroups.contains("schemaField"));
    assertTrue(searchGroups.contains("default"));
  }
}
