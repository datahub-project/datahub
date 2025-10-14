package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;

/**
 * Implementation of SearchClientShim using the Elasticsearch 7.17 REST High Level Client. This
 * implementation supports: - Elasticsearch 7.x clusters - OpenSearch 2.x clusters (which maintain
 * ES 7.x API compatibility)
 */
@Slf4j
public class Es7CompatibilitySearchClientShim extends OpenSearch2SearchClientShim {

  public Es7CompatibilitySearchClientShim(@Nonnull ShimConfiguration config) throws IOException {
    super(config);
    engineType = SearchEngineType.ELASTICSEARCH_7;
  }

  // Metadata and introspection

  @Nonnull
  @Override
  public String getEngineVersion() throws IOException {
    try {
      Map<String, String> clusterInfo = getClusterInfo();
      return clusterInfo.getOrDefault("version", "unknown");
    } catch (Exception e) {
      log.warn("Failed to get engine version", e);
      return "unknown";
    }
  }

  @Nonnull
  @Override
  public Map<String, String> getClusterInfo() throws IOException {
    try {
      // Use the info() API to get cluster information
      org.opensearch.client.core.MainResponse info = getNativeClient().info(RequestOptions.DEFAULT);

      Map<String, String> clusterInfo = new HashMap<>();
      clusterInfo.put("cluster_name", info.getClusterName());
      clusterInfo.put("cluster_uuid", info.getClusterUuid());
      clusterInfo.put("version", info.getVersion().getNumber());
      clusterInfo.put("build_flavor", info.getVersion().getBuildType());
      clusterInfo.put("build_type", info.getVersion().getBuildType());
      clusterInfo.put("build_hash", info.getVersion().getBuildHash());
      clusterInfo.put("build_date", info.getVersion().getBuildDate());
      clusterInfo.put("lucene_version", info.getVersion().getLuceneVersion());

      return clusterInfo;
    } catch (Exception e) {
      log.error("Failed to get cluster info", e);
      throw new IOException("Failed to retrieve cluster information", e);
    }
  }

  @Override
  public boolean supportsFeature(@Nonnull String feature) {
    switch (feature) {
      case "scroll":
      case "bulk":
      case "mapping_types":
        return true;
      case "point_in_time":
        // PIT is available in ES 7.10+ and OpenSearch 2.0+
        return getEngineType() == SearchEngineType.OPENSEARCH_2
            || getEngineType() == SearchEngineType.ELASTICSEARCH_7;
      case "async_search":
        // Async search is ES-specific
        return getEngineType() == SearchEngineType.ELASTICSEARCH_7;
      default:
        log.warn("Unknown feature requested: {}", feature);
        return false;
    }
  }
}
