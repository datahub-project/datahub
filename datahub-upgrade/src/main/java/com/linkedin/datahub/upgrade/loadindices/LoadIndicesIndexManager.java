package com.linkedin.datahub.upgrade.loadindices;

import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;

/**
 * Manages DataHub Elasticsearch indices during bulk loading operations. Discovers DataHub indices
 * and manages their refresh intervals and replica counts for optimal bulk loading performance. Uses
 * the same patterns as existing reindexing code with ReindexConfig and ESIndexBuilder.
 */
@Slf4j
public class LoadIndicesIndexManager {

  private static final String DISABLED_REFRESH_INTERVAL = "-1";

  private final SearchClientShim<?> searchClient;
  private final IndexConvention indexConvention;
  private final ESIndexBuilder indexBuilder;
  private List<ReindexConfig> managedIndexConfigs;

  /** -- GETTER -- Returns true if index settings are currently optimized for bulk operations. */
  @Getter private boolean settingsOptimized = false;

  private boolean indicesDiscovered = false;

  public LoadIndicesIndexManager(
      SearchClientShim<?> searchClient,
      IndexConvention indexConvention,
      ESIndexBuilder indexBuilder) {
    this.searchClient = searchClient;
    this.indexConvention = indexConvention;
    this.indexBuilder = indexBuilder;
    // Delay index discovery until first use
    this.managedIndexConfigs = new ArrayList<>();
  }

  /**
   * Discovers all DataHub indices that should have settings managed during bulk operations. This
   * includes entity indices, graph service indices, and system metadata indices since these are all
   * stored in SQL and will be modified by load indices operations. Timeseries indices are excluded
   * since they are not stored in SQL.
   *
   * @return List of ReindexConfig objects for managed indices
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public List<ReindexConfig> discoverDataHubIndexConfigs() throws IOException {
    List<ReindexConfig> configs = new ArrayList<>();

    // Get entity indices using IndexConvention pattern
    String entityPattern = indexConvention.getAllEntityIndicesPattern();
    log.debug("Querying entity indices with pattern: {}", entityPattern);
    GetIndexRequest entityRequest = new GetIndexRequest(entityPattern);
    GetIndexResponse entityResponse = searchClient.getIndex(entityRequest, RequestOptions.DEFAULT);
    String[] entityIndices = entityResponse.getIndices();

    for (String indexName : entityIndices) {
      try {
        ReindexConfig config = indexBuilder.buildReindexState(indexName, Map.of(), Map.of());
        configs.add(config);
        log.debug("Added entity index config: {}", indexName);
      } catch (IOException e) {
        log.warn(
            "Failed to build reindex config for entity index {}: {}", indexName, e.getMessage());
      }
    }

    // Get graph service index
    String graphIndexName = indexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME);
    log.debug("Querying graph service index: {}", graphIndexName);
    GetIndexRequest graphRequest = new GetIndexRequest(graphIndexName);
    try {
      GetIndexResponse graphResponse = searchClient.getIndex(graphRequest, RequestOptions.DEFAULT);
      String[] graphIndices = graphResponse.getIndices();
      for (String indexName : graphIndices) {
        try {
          ReindexConfig config = indexBuilder.buildReindexState(indexName, Map.of(), Map.of());
          configs.add(config);
          log.debug("Added graph service index config: {}", indexName);
        } catch (IOException e) {
          log.warn(
              "Failed to build reindex config for graph index {}: {}", indexName, e.getMessage());
        }
      }
    } catch (Exception e) {
      log.debug(
          "Graph service index {} does not exist or is not accessible: {}",
          graphIndexName,
          e.getMessage());
    }

    // Get system metadata index
    String systemMetadataIndexName =
        indexConvention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME);
    log.debug("Querying system metadata index: {}", systemMetadataIndexName);
    GetIndexRequest systemMetadataRequest = new GetIndexRequest(systemMetadataIndexName);
    try {
      GetIndexResponse systemMetadataResponse =
          searchClient.getIndex(systemMetadataRequest, RequestOptions.DEFAULT);
      String[] systemMetadataIndices = systemMetadataResponse.getIndices();
      for (String indexName : systemMetadataIndices) {
        try {
          ReindexConfig config = indexBuilder.buildReindexState(indexName, Map.of(), Map.of());
          configs.add(config);
          log.debug("Added system metadata index config: {}", indexName);
        } catch (IOException e) {
          log.warn(
              "Failed to build reindex config for system metadata index {}: {}",
              indexName,
              e.getMessage());
        }
      }
    } catch (Exception e) {
      log.debug(
          "System metadata index {} does not exist or is not accessible: {}",
          systemMetadataIndexName,
          e.getMessage());
    }

    return configs;
  }

  /**
   * Optimizes index settings for bulk operations by disabling refresh and setting replicas to zero.
   */
  public void optimizeForBulkOperations() throws IOException {
    if (settingsOptimized) {
      log.warn("Index settings are already optimized for bulk operations");
      return;
    }

    // Discover indices lazily on first use (after BuildIndicesStep has run)
    if (!indicesDiscovered) {
      log.info("Discovering DataHub indices for settings optimization...");
      this.managedIndexConfigs = discoverDataHubIndexConfigs();
      this.indicesDiscovered = true;
    }

    log.info("Optimizing settings for bulk operations on {} indices", managedIndexConfigs.size());

    for (ReindexConfig config : managedIndexConfigs) {
      try {
        // Disable refresh interval for bulk operations
        indexBuilder.setIndexRefreshInterval(config.name(), DISABLED_REFRESH_INTERVAL);

        indexBuilder.tweakReplicas(config, false);

        log.debug("Optimized settings for index: {}", config.name());
      } catch (IOException e) {
        log.error("Failed to optimize settings for index: {}", config.name(), e);
        throw e;
      }
    }

    settingsOptimized = true;
    log.info("Successfully optimized settings for bulk operations on all managed indices");
  }

  /** Restores index settings to configured values for all managed indices. */
  public void restoreFromConfiguration() throws IOException {
    if (!settingsOptimized) {
      log.warn("Index settings are not currently optimized");
      return;
    }

    log.info("Restoring settings to configured values for {} indices", managedIndexConfigs.size());

    for (ReindexConfig config : managedIndexConfigs) {
      try {
        // Get target settings from ReindexConfig (includes per-index overrides)
        Map<String, Object> targetSettings = config.targetSettings();
        Map<String, Object> indexSettings = (Map<String, Object>) targetSettings.get("index");

        // Extract refresh interval and replica count from target settings
        String targetRefreshInterval = (String) indexSettings.get(ESIndexBuilder.REFRESH_INTERVAL);
        Integer targetReplicaCount = (Integer) indexSettings.get(ESIndexBuilder.NUMBER_OF_REPLICAS);

        // Restore refresh interval to target value (includes per-index overrides)
        indexBuilder.setIndexRefreshInterval(config.name(), targetRefreshInterval);

        // Restore replica count to target value (includes per-index overrides)
        indexBuilder.setIndexReplicaCount(config.name(), targetReplicaCount);

        log.debug(
            "Restored settings for index: {} to refresh: {}, replicas: {}",
            config.name(),
            targetRefreshInterval,
            targetReplicaCount);
      } catch (IOException e) {
        log.error("Failed to restore settings for index: {}", config.name(), e);
        throw e;
      }
    }

    settingsOptimized = false;
    log.info("Successfully restored settings to configured values for all managed indices");
  }
}
