package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Test to verify that primary entities have the correct searchGroup set to "primary" in the main
 * entity-registry.yaml file.
 */
public class PrimaryEntitySearchGroupTest {

  @BeforeTest
  public void disableAssert() {
    com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(
            com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor.class
                .getName(),
            false);
  }

  // Expected primary entities as specified in the user's requirements
  private static final List<String> EXPECTED_PRIMARY_ENTITIES =
      Arrays.asList(
          "role",
          "dataset",
          "dataJob",
          "dataFlow",
          "chart",
          "dashboard",
          "notebook",
          "corpuser",
          "corpGroup",
          "domain",
          "container",
          "tag",
          "glossaryTerm",
          "glossaryNode",
          "mlModel",
          "mlModelGroup",
          "mlFeatureTable",
          "mlFeature",
          "mlPrimaryKey",
          "dataHubRole",
          "businessAttribute",
          "dataContract",
          "dataProduct",
          "application");

  @Test
  public void testPrimaryEntitiesHaveCorrectSearchGroup() throws IOException {
    // Load the main entity registry
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    assertNotNull(entitySpecs, "Entity specs should not be null");

    // Test each expected primary entity
    for (String entityName : EXPECTED_PRIMARY_ENTITIES) {
      EntitySpec entitySpec = configEntityRegistry.getEntitySpec(entityName);
      assertNotNull(
          entitySpec, String.format("Entity spec for '%s' should not be null", entityName));

      String searchGroup = entitySpec.getSearchGroup();
      assertEquals(
          searchGroup,
          "primary",
          String.format(
              "Entity '%s' should have searchGroup 'primary', but got '%s'",
              entityName, searchGroup));
    }
  }

  @Test
  public void testPrimaryEntitiesAreInPrimarySearchGroup() throws IOException {
    // Load the main entity registry
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    // Get all entities in the primary search group
    Map<String, EntitySpec> primaryEntities =
        configEntityRegistry.getEntitySpecsBySearchGroup("primary");
    assertNotNull(primaryEntities, "Primary entities map should not be null");

    // Verify that all expected primary entities are in the primary search group
    for (String entityName : EXPECTED_PRIMARY_ENTITIES) {
      assertNotNull(
          primaryEntities.get(entityName.toLowerCase()),
          String.format("Entity '%s' should be in primary search group", entityName));
    }

    // Log the actual primary entities for debugging
    System.out.println("Entities in primary search group:");
    primaryEntities.keySet().stream().sorted().forEach(name -> System.out.println("  - " + name));
  }

  @Test
  public void testNoPrimaryEntitiesHaveDefaultSearchGroup() throws IOException {
    // Load the main entity registry
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    // Get all entities in the default search group
    Map<String, EntitySpec> defaultEntities =
        configEntityRegistry.getEntitySpecsBySearchGroup("default");
    assertNotNull(defaultEntities, "Default entities map should not be null");

    // Verify that none of the expected primary entities are in the default search group
    for (String entityName : EXPECTED_PRIMARY_ENTITIES) {
      EntitySpec entitySpec = defaultEntities.get(entityName.toLowerCase());
      if (entitySpec != null) {
        throw new AssertionError(
            String.format(
                "Entity '%s' should NOT be in default search group, but it is", entityName));
      }
    }
  }

  @Test
  public void testSearchGroupValues() throws IOException {
    // Load the main entity registry
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();

    // Log all search groups for debugging
    System.out.println("All search groups in entity registry:");
    configEntityRegistry.getSearchGroups().stream()
        .sorted()
        .forEach(
            group -> {
              Map<String, EntitySpec> entitiesInGroup =
                  configEntityRegistry.getEntitySpecsBySearchGroup(group);
              System.out.println("  " + group + " (" + entitiesInGroup.size() + " entities):");
              entitiesInGroup.keySet().stream()
                  .sorted()
                  .forEach(name -> System.out.println("    - " + name));
            });
  }
}
