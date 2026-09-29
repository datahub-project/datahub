package com.linkedin.metadata.utils.elasticsearch;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.common.settings.Settings;

/**
 * Compares authored index analysis settings against values persisted in Elasticsearch/OpenSearch.
 * Engine-specific shims implement this to normalize round-trip representation differences.
 */
public interface IndexSettingsComparison {

  /**
   * Stored setting keys to compare against the authored target at one nesting level of index
   * analysis settings. Engine-specific shims may ignore keys injected on persistence round-trip.
   */
  @Nonnull
  default Set<String> indexSettingNamesForComparison(
      @Nonnull Map<String, Object> targetSettings, @Nonnull Settings storedSettings) {
    return storedSettings.names();
  }

  /**
   * Compare scalar values from authored target settings with stored index settings strings.
   *
   * <p>Engine-specific shims may normalize values that differ only by representation on round-trip.
   */
  default boolean indexSettingValuesEqual(
      @Nullable Object targetValue, @Nullable String storedValue) {
    if (targetValue == null && storedValue == null) {
      return true;
    }
    if (targetValue == null || storedValue == null) {
      return false;
    }
    return targetValue.toString().equals(storedValue);
  }

  /** Strict comparison with no engine-specific normalization. */
  enum Strict implements IndexSettingsComparison {
    INSTANCE
  }
}
