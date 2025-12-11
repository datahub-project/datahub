/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
