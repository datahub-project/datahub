package com.linkedin.metadata.search.elasticsearch.index;

import com.linkedin.metadata.config.search.IndexConfiguration;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Interface for building Elasticsearch settings for entities. This interface defines the contract
 * for classes that build Elasticsearch index settings.
 */
public interface SettingsBuilder {

  /**
   * Builds settings for a specific index.
   *
   * @param indexConfiguration index configuration
   * @param indexName the name of the index to get settings for
   * @return settings for the specified index, or empty map if not found
   */
  Map<String, Object> getSettings(
      @Nonnull IndexConfiguration indexConfiguration, @Nonnull String indexName);
}
