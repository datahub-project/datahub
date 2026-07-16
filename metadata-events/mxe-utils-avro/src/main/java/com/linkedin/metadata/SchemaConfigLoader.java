package com.linkedin.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.io.InputStream;

/**
 * Loads schema configuration from YAML file. This class is responsible only for loading the
 * configuration data.
 */
public class SchemaConfigLoader {

  private final SchemaConfig config;

  public SchemaConfigLoader(ObjectMapper objectMapper) {
    this.config = loadConfig(objectMapper);
  }

  private SchemaConfig loadConfig(ObjectMapper objectMapper) {
    try {
      // Use provided ObjectMapper or create YAML-enabled one
      ObjectMapper yamlMapper =
          objectMapper.getFactory() instanceof YAMLFactory
              ? objectMapper
              : new ObjectMapper(new YAMLFactory());

      InputStream inputStream =
          getClass().getClassLoader().getResourceAsStream("schema-config.yaml");

      if (inputStream != null) {
        return yamlMapper.readValue(inputStream, SchemaConfig.class);
      } else {
        throw new RuntimeException("schema-config.yaml not found in classpath");
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema configuration from YAML", e);
    }
  }

  public SchemaConfig getConfig() {
    return config;
  }

  // Inner classes for YAML deserialization

  public static class SchemaConfig {
    private java.util.Map<String, SchemaDefinition> schemas;

    public java.util.Map<String, SchemaDefinition> getSchemas() {
      return schemas;
    }

    public void setSchemas(java.util.Map<String, SchemaDefinition> schemas) {
      this.schemas = schemas;
    }
  }

  public static class SchemaDefinition {
    private String description;

    private java.util.List<VersionDefinition> versions;
    private String compatibility;

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public java.util.List<VersionDefinition> getVersions() {
      return versions;
    }

    public void setVersions(java.util.List<VersionDefinition> versions) {
      this.versions = versions;
    }

    public String getCompatibility() {
      return compatibility;
    }

    public void setCompatibility(String compatibility) {
      this.compatibility = compatibility;
    }
  }

  public static class VersionDefinition {
    private int version;

    @JsonProperty("ordinal_id")
    private String ordinalId;

    private String description;

    public int getVersion() {
      return version;
    }

    public void setVersion(int version) {
      this.version = version;
    }

    public String getOrdinalId() {
      return ordinalId;
    }

    public void setOrdinalId(String ordinalId) {
      this.ordinalId = ordinalId;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }
  }
}
