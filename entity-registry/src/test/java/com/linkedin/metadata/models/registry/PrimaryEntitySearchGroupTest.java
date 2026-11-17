package com.linkedin.metadata.models.registry;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.EntitySpec;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * OSS entity-registry.yml omits searchGroup so V3 uses entity-named indices. SaaS overlays set
 * groups.
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

  private static final List<String> CORE_ENTITIES =
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
  public void testOssEntitiesHaveUnsetSearchGroup() throws IOException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    Map<String, EntitySpec> entitySpecs = configEntityRegistry.getEntitySpecs();
    assertNotNull(entitySpecs, "Entity specs should not be null");

    for (String entityName : CORE_ENTITIES) {
      EntitySpec entitySpec = configEntityRegistry.getEntitySpec(entityName);
      assertNotNull(
          entitySpec, String.format("Entity spec for '%s' should not be null", entityName));
      assertNull(
          entitySpec.getSearchGroup(),
          String.format(
              "OSS entity '%s' should have unset searchGroup, but got '%s'",
              entityName, entitySpec.getSearchGroup()));
    }
  }

  @Test
  public void testOssRegistryHasNoExplicitSearchGroups() throws IOException {
    ConfigEntityRegistry configEntityRegistry =
        new ConfigEntityRegistry(
            PrimaryEntitySearchGroupTest.class
                .getClassLoader()
                .getResourceAsStream("entity-registry.yml"));

    assertTrue(
        configEntityRegistry.getSearchGroups().isEmpty(),
        "OSS entity-registry.yml should not assign searchGroups");
  }
}
