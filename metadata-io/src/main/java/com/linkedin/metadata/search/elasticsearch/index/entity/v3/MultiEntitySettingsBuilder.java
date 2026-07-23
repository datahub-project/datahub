package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.BaseConfigurationLoader;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

/** Builder for generating settings for elasticsearch indices with entity-based field structure */
public class MultiEntitySettingsBuilder implements SettingsBuilder {

  public final Map<String, Object> settings;
  private final Map<String, Object> analyzerConfiguration;
  private final Integer maxFieldsLimit;
  @Nonnull private final IndexConvention indexConvention;

  /**
   * Creates a SettingsBuilder with analyzer configuration loaded from a resource path. This
   * constructor should be used when the analyzer configuration path comes from application
   * configuration.
   *
   * @param entityIndexConfiguration the entity index configuration
   * @param indexConvention the index convention for name validation (required)
   * @throws IOException if the configuration resource cannot be read
   */
  public MultiEntitySettingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nonnull IndexConvention indexConvention)
      throws IOException {
    this.indexConvention = indexConvention;
    this.maxFieldsLimit = entityIndexConfiguration.getV3().getMaxFieldsLimit();

    if (!entityIndexConfiguration.getV3().getAnalyzerConfig().trim().isEmpty()) {
      this.analyzerConfiguration =
          loadAnalyzerConfigurationFromResource(
              entityIndexConfiguration.getV3().getAnalyzerConfig());
    } else {
      this.analyzerConfiguration = null;
    }

    settings = buildSettings();
  }

  @Override
  public Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName) {
    // For v3, only apply settings to indices that match the v3 entity naming pattern
    if (!indexConvention.isV3EntityIndex(indexName)) {
      // Return empty settings if this is not a v3 entity index
      return new HashMap<>();
    }
    return buildSettings();
  }

  /**
   * Loads analyzer configuration from a resource. Supports both JSON and YAML formats based on
   * resource extension.
   *
   * @param resourcePath resource path to the configuration
   * @return Map containing the analyzer configuration
   * @throws IOException if the resource cannot be read
   */
  private static Map<String, Object> loadAnalyzerConfigurationFromResource(String resourcePath)
      throws IOException {
    Map<String, Object> config =
        BaseConfigurationLoader.loadConfigurationFromResource(resourcePath);
    return BaseConfigurationLoader.extractAnalysisSection(config, resourcePath);
  }

  /**
   * Builds minimal settings for simple field types. Since we're only using keyword, boolean, etc.,
   * we don't need complex analysis.
   */
  private Map<String, Object> buildSettings() {
    Map<String, Object> baseSettings = new HashMap<>();

    // Add analysis configuration if available
    if (analyzerConfiguration != null) {
      baseSettings.put("analysis", analyzerConfiguration);
      // Set field limit for v3 indices to handle many aspects and fields
      if (maxFieldsLimit != null) {
        baseSettings.put("mapping.total_fields.limit", maxFieldsLimit);
      }
    }

    return baseSettings;
  }
}
