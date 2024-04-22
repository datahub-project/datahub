package com.linkedin.metadata.shared;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public interface ElasticSearchIndexed {
  /**
   * The index configurations for the given service.
   *
   * @return List of reindex configurations
   */
  List<ReindexConfig> buildReindexConfigs() throws IOException;

  /**
   * The index configurations for the given service with StructuredProperties applied.
   *
   * @param properties The structured properties to apply to the index mappings
   * @return List of reindex configurations
   */
  List<ReindexConfig> buildReindexConfigsWithAllStructProps(
      Collection<StructuredPropertyDefinition> properties) throws IOException;

  /**
   * Mirrors the service's functions which are expected to build/reindex as needed based on the
   * reindex configurations above
   */
  void reindexAll() throws IOException;
}
