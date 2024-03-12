package com.linkedin.metadata.shared;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import java.io.IOException;
import java.util.List;

public interface ElasticSearchIndexed {
  /**
   * The index configurations for the given service.
   *
   * @return List of reindex configurations
   */
  List<ReindexConfig> buildReindexConfigs() throws IOException;

  /**
   * Mirrors the service's functions which are expected to build/reindex as needed based on the
   * reindex configurations above
   */
  void reindexAll() throws IOException;
}
