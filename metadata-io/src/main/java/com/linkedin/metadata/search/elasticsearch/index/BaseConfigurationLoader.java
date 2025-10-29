package com.linkedin.metadata.search.elasticsearch.index;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for loading configuration files (YAML/JSON) from resources. Consolidates
 * configuration loading functionality used by both MappingsBuilder and SettingsBuilder.
 */
public class BaseConfigurationLoader {

  private BaseConfigurationLoader() {
    // Utility class, no instantiation
  }

  /**
   * Loads configuration from a resource. Supports both JSON and YAML formats based on resource
   * extension.
   *
   * @param resourcePath resource path to the configuration
   * @return Map containing the configuration
   * @throws IOException if the resource cannot be read
   */
  public static Map<String, Object> loadConfigurationFromResource(String resourcePath)
      throws IOException {
    ObjectMapper mapper;
    if (resourcePath.toLowerCase().endsWith(".yaml")
        || resourcePath.toLowerCase().endsWith(".yml")) {
      mapper = new ObjectMapper(new YAMLFactory());
    } else {
      mapper = new ObjectMapper();
    }

    try (InputStream inputStream =
        BaseConfigurationLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Configuration resource not found: " + resourcePath);
      }

      return mapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
    }
  }

  /**
   * Extracts the analysis section from analyzer configuration. This method handles the nested
   * structure of the analyzer configuration file.
   *
   * @param config the loaded configuration
   * @param source the source path/resource for error reporting
   * @return Map containing the analysis configuration
   * @throws IOException if the analysis section cannot be found
   */
  public static Map<String, Object> extractAnalysisSection(
      Map<String, Object> config, String source) throws IOException {
    // Extract the analysis section from the first index settings
    for (Map.Entry<String, Object> entry : config.entrySet()) {
      if (entry.getValue() instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> indexConfig = (Map<String, Object>) entry.getValue();
        if (indexConfig.containsKey("settings")) {
          @SuppressWarnings("unchecked")
          Map<String, Object> settings = (Map<String, Object>) indexConfig.get("settings");
          if (settings.containsKey("index")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
            if (indexSettings.containsKey("analysis")) {
              return (Map<String, Object>) indexSettings.get("analysis");
            }
          }
        }
      }
    }

    throw new IOException("No analysis configuration found in: " + source);
  }

  /**
   * Extracts the mappings section from mapping configuration. This method handles the nested
   * structure of the mapping configuration file.
   *
   * @param config the loaded configuration
   * @param source the source path/resource for error reporting
   * @return Map containing the mappings configuration
   * @throws IOException if the mappings section cannot be found
   */
  public static Map<String, Object> extractMappingsSection(
      Map<String, Object> config, String source) throws IOException {
    if (config == null) {
      throw new IOException("Configuration is null for source: " + source);
    }

    Object entityMappingConfig = config.get("entity_mapping_config");
    if (entityMappingConfig == null) {
      throw new IOException("Missing 'entity_mapping_config' section in configuration: " + source);
    }

    if (!(entityMappingConfig instanceof Map)) {
      throw new IOException(
          "'entity_mapping_config' section is not a map in configuration: " + source);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> entityMappingConfigMap = (Map<String, Object>) entityMappingConfig;

    Object mappings = entityMappingConfigMap.get("mappings");
    if (mappings == null) {
      throw new IOException("Missing 'mappings' section in entity_mapping_config: " + source);
    }

    if (!(mappings instanceof Map)) {
      throw new IOException("'mappings' section is not a map in entity_mapping_config: " + source);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> mappingsMap = (Map<String, Object>) mappings;

    return new HashMap<>(mappingsMap);
  }
}
