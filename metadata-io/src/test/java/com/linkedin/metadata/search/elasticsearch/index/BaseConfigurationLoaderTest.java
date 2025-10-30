package com.linkedin.metadata.search.elasticsearch.index;

import static org.testng.Assert.*;

import java.io.IOException;
import java.util.Map;
import org.testng.annotations.Test;

/** Tests for ConfigurationLoader utility class. */
public class BaseConfigurationLoaderTest {

  @Test
  public void testLoadConfigurationFromResource() throws Exception {
    // Test that configuration can be loaded from a resource

    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource("search_entity_mapping_config.yaml");

    assertNotNull(config, "Configuration should be loaded");
    assertTrue(
        config.containsKey("entity_mapping_config"),
        "Should contain entity_mapping_config section");
  }

  @Test
  public void testExtractMappingsSection() throws Exception {
    // Test that mappings section can be extracted from configuration

    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource("search_entity_mapping_config.yaml");
    Map<String, Object> mappings = BaseConfigurationLoader.extractMappingsSection(config, "test");

    assertNotNull(mappings, "Mappings should be extracted");
    assertTrue(mappings.containsKey("dynamic"), "Should contain dynamic setting");
    assertTrue(mappings.containsKey("dynamic_templates"), "Should contain dynamic templates");
    assertTrue(mappings.containsKey("properties"), "Should contain properties");
  }

  @Test
  public void testExtractAnalysisSection() throws Exception {
    // Test that analysis section can be extracted from analyzer configuration

    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource("search_entity_analyzer_config.yaml");
    Map<String, Object> analysis = BaseConfigurationLoader.extractAnalysisSection(config, "test");

    assertNotNull(analysis, "Analysis should be extracted");
    assertTrue(analysis.containsKey("analyzer"), "Should contain analyzer section");
    assertTrue(analysis.containsKey("normalizer"), "Should contain normalizer section");
  }

  @Test(expectedExceptions = IOException.class)
  public void testExtractMappingsSectionWithMissingEntityMappingConfig() throws Exception {
    // Test that error is thrown when entity_mapping_config is missing

    Map<String, Object> config = Map.of("other_section", Map.of("key", "value"));
    BaseConfigurationLoader.extractMappingsSection(config, "test");
  }

  @Test(expectedExceptions = IOException.class)
  public void testExtractMappingsSectionWithMissingMappings() throws Exception {
    // Test that error is thrown when mappings section is missing

    Map<String, Object> config = Map.of("entity_mapping_config", Map.of("other", "value"));
    BaseConfigurationLoader.extractMappingsSection(config, "test");
  }

  @Test(expectedExceptions = IOException.class)
  public void testExtractAnalysisSectionWithNoAnalysis() throws Exception {
    // Test that error is thrown when no analysis section is found

    Map<String, Object> config = Map.of("other_section", Map.of("key", "value"));
    BaseConfigurationLoader.extractAnalysisSection(config, "test");
  }

  @Test
  public void testLoadConfigurationFromResourceNotFound() {
    // Test that appropriate error is thrown for non-existent resource

    assertThrows(
        IOException.class,
        () -> {
          BaseConfigurationLoader.loadConfigurationFromResource("non_existent_file.yaml");
        });
  }
}
