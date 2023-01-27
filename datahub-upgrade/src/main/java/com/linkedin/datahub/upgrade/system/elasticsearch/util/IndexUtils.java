package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class IndexUtils {
  private IndexUtils() { }

  private static List<ReindexConfig> _reindexConfigs = new ArrayList<>();

  public static List<ReindexConfig> getAllReindexConfigs(List<ElasticSearchIndexed> elasticSearchIndexedList) throws IOException {
    // Avoid locking & reprocessing
    List<ReindexConfig> reindexConfigs = new ArrayList<>(_reindexConfigs);
    if (reindexConfigs.isEmpty()) {
      for (ElasticSearchIndexed elasticSearchIndexed : elasticSearchIndexedList) {
        reindexConfigs.addAll(elasticSearchIndexed.getReindexConfigs());
      }
      _reindexConfigs = new ArrayList<>(reindexConfigs);
    }

    return reindexConfigs;
  }
}
