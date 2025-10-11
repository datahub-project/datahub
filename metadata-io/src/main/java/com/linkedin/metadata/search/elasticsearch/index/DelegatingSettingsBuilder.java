package com.linkedin.metadata.search.elasticsearch.index;

import com.google.common.collect.Maps;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.LegacySettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntitySettingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Delegating SettingsBuilder that forwards calls to the appropriate v2 or v3 implementation based
 * on the EntityIndexConfiguration flags. Both v2 and v3 can be enabled simultaneously.
 */
@Slf4j
public class DelegatingSettingsBuilder implements SettingsBuilder {

  private final List<SettingsBuilder> builders;
  private final boolean v2Enabled;
  private final boolean v3Enabled;
  @Nonnull private final IndexConvention indexConvention;

  public DelegatingSettingsBuilder(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration,
      @Nonnull IndexConfiguration indexConfiguration,
      @Nonnull IndexConvention indexConvention) {

    this.v2Enabled =
        entityIndexConfiguration.getV2() != null && entityIndexConfiguration.getV2().isEnabled();
    this.v3Enabled =
        entityIndexConfiguration.getV3() != null && entityIndexConfiguration.getV3().isEnabled();
    this.indexConvention = indexConvention;

    this.builders = new ArrayList<>();

    if (v2Enabled) {
      this.builders.add(new LegacySettingsBuilder(indexConfiguration, indexConvention));
    }

    if (v3Enabled) {
      try {
        this.builders.add(
            new MultiEntitySettingsBuilder(entityIndexConfiguration, indexConvention));
      } catch (IOException e) {
        log.error("Failed to initialize MultiEntitySettingsBuilder", e);
        throw new RuntimeException("Failed to initialize MultiEntitySettingsBuilder", e);
      }
    }

    if (this.builders.isEmpty()) {
      log.warn(
          "Neither v2 nor v3 entity index is enabled. SettingsBuilder will return empty settings.");
    }
  }

  @Override
  public Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName) {
    if (builders.isEmpty()) {
      log.warn("No settings builders available. Returning empty settings for index: {}", indexName);
      return new HashMap<>();
    }

    // Collect settings from all builders, filtering out empty ones
    List<Map<String, Object>> nonEmptySettings = new ArrayList<>();
    List<String> builderNames = new ArrayList<>();

    for (SettingsBuilder builder : builders) {
      try {
        Map<String, Object> settings = builder.getSettings(indexConfiguration, indexName);
        if (!settings.isEmpty()) {
          nonEmptySettings.add(settings);
          builderNames.add(builder.getClass().getSimpleName());
        }
      } catch (Exception e) {
        log.debug(
            "Builder {} failed for index {}: {}",
            builder.getClass().getSimpleName(),
            indexName,
            e.getMessage());
        // Continue with other builders
      }
    }

    // If no builders returned non-empty settings, return empty settings
    if (nonEmptySettings.isEmpty()) {
      log.debug("No builders returned non-empty settings for index: {}", indexName);
      return new HashMap<>();
    }

    // If only one builder returned settings, use those
    if (nonEmptySettings.size() == 1) {
      Map<String, Object> settings = nonEmptySettings.get(0);
      log.debug(
          "Settings from {}: {} settings for index: {}",
          builderNames.get(0),
          settings.size(),
          indexName);
      return settings;
    }

    // Multiple builders returned settings - validate they are consistent
    Map<String, Object> referenceSettings = nonEmptySettings.get(0);
    String referenceBuilderName = builderNames.get(0);

    for (int i = 1; i < nonEmptySettings.size(); i++) {
      Map<String, Object> settings = nonEmptySettings.get(i);
      String builderName = builderNames.get(i);

      if (!settingsEqual(referenceSettings, settings)) {
        log.error(
            "Inconsistent settings detected between {} and {} for index: {}",
            referenceBuilderName,
            builderName,
            indexName);
        log.error("Reference settings: {}", referenceSettings);
        log.error("Conflicting settings: {}", settings);
        throw new IllegalStateException(
            String.format(
                "Inconsistent settings between %s and %s for index %s. This indicates an illegal configuration state.",
                referenceBuilderName, builderName, indexName));
      }
      log.debug("Validated settings from {}: {} settings", builderName, settings.size());
    }

    return referenceSettings;
  }

  /**
   * Compares two maps for equality using Google Guava's Maps.difference(). This performs deep
   * comparison of nested maps and collections.
   */
  private boolean settingsEqual(Map<String, Object> map1, Map<String, Object> map2) {
    if (map1 == map2) {
      return true;
    }
    if (map1 == null || map2 == null) {
      return false;
    }

    return Maps.difference(map1, map2).areEqual();
  }
}
