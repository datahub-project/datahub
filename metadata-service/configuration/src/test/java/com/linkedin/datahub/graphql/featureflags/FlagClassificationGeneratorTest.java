package com.linkedin.datahub.graphql.featureflags;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class FlagClassificationGeneratorTest {

  // camelToUpperUnderscore is package-private — accessible from this test package

  @Test
  public void testCamelToUpperUnderscore_simpleTransitions() {
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("showBrowseV2"), "SHOW_BROWSE_V2");
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("readOnlyModeEnabled"),
        "READ_ONLY_MODE_ENABLED");
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("lineageSearchCacheEnabled"),
        "LINEAGE_SEARCH_CACHE_ENABLED");
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("entityVersioning"),
        "ENTITY_VERSIONING");
  }

  @Test
  public void testCamelToUpperUnderscore_acronyms() {
    // Uppercase runs (acronyms) should be kept together, with underscore on either side
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("alternateMCPValidation"),
        "ALTERNATE_MCP_VALIDATION");
    assertEquals(
        FlagClassificationGenerator.camelToUpperUnderscore("schemaFieldCLLEnabled"),
        "SCHEMA_FIELD_CLL_ENABLED");
  }

  @Test
  public void testGeneratorOutputContainsAllFeatureFlagsBooleans() throws Exception {
    Path output = Files.createTempFile("flag-classification-test", ".json");
    try {
      FlagClassificationGenerator.main(new String[] {output.toString()});

      JsonNode root = new ObjectMapper().readTree(output.toFile());

      // Top-level structure
      assertNotNull(root.get("_generated_at"), "Missing _generated_at");
      assertNotNull(root.get("dynamic"), "Missing dynamic section");
      assertNotNull(root.get("static"), "Missing static section");

      JsonNode dynamic = root.get("dynamic");

      // Spot-check a handful of known FeatureFlags.java boolean fields
      assertTrue(dynamic.has("SHOW_BROWSE_V2"), "Missing SHOW_BROWSE_V2");
      assertTrue(dynamic.has("ENTITY_VERSIONING"), "Missing ENTITY_VERSIONING");
      assertTrue(
          dynamic.has("LINEAGE_SEARCH_CACHE_ENABLED"), "Missing LINEAGE_SEARCH_CACHE_ENABLED");

      // Each entry must carry field name and default value
      JsonNode browsev2 = dynamic.get("SHOW_BROWSE_V2");
      assertEquals(browsev2.get("field").asText(), "showBrowseV2");
      assertNotNull(browsev2.get("default"));
    } finally {
      Files.deleteIfExists(output);
    }
  }
}
