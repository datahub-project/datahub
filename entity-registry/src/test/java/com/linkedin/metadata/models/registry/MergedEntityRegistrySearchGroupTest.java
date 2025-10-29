package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import java.io.IOException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Test to verify that MergedEntityRegistry properly handles searchGroup conflicts when merging
 * multiple entity registries.
 */
public class MergedEntityRegistrySearchGroupTest {

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
  public void testMergeWithSameSearchGroup() throws IOException, EntityRegistryException {
    // Create base registry with dataset having searchGroup "primary"
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with dataset also having searchGroup "primary"
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: primary
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that dataset still has searchGroup "primary"
    EntitySpec datasetSpec = mergedRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec);
    assertEquals(datasetSpec.getSearchGroup(), "primary");
  }

  @Test
  public void testMergeWithDefaultAndPrimarySearchGroup()
      throws IOException, EntityRegistryException {
    // Create base registry with dataset having searchGroup "primary"
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with dataset having no searchGroup (defaults to "default")
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that dataset keeps the "primary" searchGroup from base registry
    EntitySpec datasetSpec = mergedRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec);
    assertEquals(datasetSpec.getSearchGroup(), "primary");
  }

  @Test
  public void testMergeWithPrimaryAndDefaultSearchGroup()
      throws IOException, EntityRegistryException {
    // Create base registry with dataset having no searchGroup (defaults to "default")
    String baseYaml =
        """
        id: base-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(baseYaml.getBytes()));

    // Create patch registry with dataset having searchGroup "primary"
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: primary
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that dataset gets the "primary" searchGroup from patch registry
    EntitySpec datasetSpec = mergedRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec);
    assertEquals(datasetSpec.getSearchGroup(), "primary");
  }

  @Test(expectedExceptions = EntityRegistryException.class)
  public void testMergeWithConflictingSearchGroups() throws IOException, EntityRegistryException {
    // Create base registry with dataset having searchGroup "primary"
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with dataset having searchGroup "timeseries"
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: timeseries
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries - this should throw an exception
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);
  }

  @Test
  public void testMergeWithNewEntityHavingSearchGroup()
      throws IOException, EntityRegistryException {
    // Create base registry
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with a new entity having searchGroup "primary"
    // Use existing aspects from the test registry
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: newEntity
            keyAspect: datasetKey
            searchGroup: primary
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that the new entity has the correct searchGroup
    EntitySpec newEntitySpec = mergedRegistry.getEntitySpec("newEntity");
    assertNotNull(newEntitySpec);
    assertEquals(newEntitySpec.getSearchGroup(), "primary");
  }

  @Test(expectedExceptions = EntityRegistryException.class)
  public void testMergeWithMultipleConflictingEntities()
      throws IOException, EntityRegistryException {
    // Create base registry
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with multiple entities having conflicting searchGroups
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            searchGroup: timeseries
            aspects:
              - datasetProperties
          - name: chart
            keyAspect: chartKey
            searchGroup: query
            aspects:
              - chartInfo
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries - this should throw an exception
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);
  }

  @Test
  public void testSearchGroupPrecedenceRules() throws IOException, EntityRegistryException {
    // Test that when one registry has default and another has a specific searchGroup,
    // the specific one takes precedence

    // Create base registry with dataset having searchGroup "primary"
    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(
            MergedEntityRegistrySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("test-search-index-group-entity-registry.yml"));

    // Create patch registry with dataset having no searchGroup (defaults to "default")
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: dataset
            keyAspect: datasetKey
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that dataset keeps the "primary" searchGroup (non-default takes precedence)
    EntitySpec datasetSpec = mergedRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec);
    assertEquals(datasetSpec.getSearchGroup(), "primary");
  }

  @Test
  public void testSearchGroupValidationWithDefaultValues()
      throws IOException, EntityRegistryException {
    // Test that entities with default searchGroup don't conflict with each other

    // Create base registry with entity having no searchGroup
    String baseYaml =
        """
        id: base-registry
        entities:
          - name: testEntity2
            keyAspect: datasetKey
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry baseRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(baseYaml.getBytes()));

    // Create patch registry with same entity also having no searchGroup
    String patchYaml =
        """
        id: patch-registry
        entities:
          - name: testEntity2
            keyAspect: datasetKey
            aspects:
              - datasetProperties
        """;

    ConfigEntityRegistry patchRegistry =
        new ConfigEntityRegistry(new java.io.ByteArrayInputStream(patchYaml.getBytes()));

    // Merge the registries - this should succeed
    MergedEntityRegistry mergedRegistry = new MergedEntityRegistry(baseRegistry);
    mergedRegistry.apply(patchRegistry);

    // Verify that testEntity2 has default searchGroup
    EntitySpec testEntitySpec = mergedRegistry.getEntitySpec("testEntity2");
    assertNotNull(testEntitySpec);
    assertEquals(testEntitySpec.getSearchGroup(), EntityAnnotation.DEFAULT_SEARCH_GROUP);
  }
}
