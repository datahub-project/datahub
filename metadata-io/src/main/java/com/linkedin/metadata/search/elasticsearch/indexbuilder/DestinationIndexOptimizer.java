package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.ReindexIOException;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.ReplicaHealthException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.common.settings.Settings;

/**
 * Manages transactional optimization of Elasticsearch destination indices during reindexing.
 *
 * <p>Applies performance optimizations before reindex: - refresh_interval = -1 (disable refreshes,
 * reduce segment creation) or 30s/60s based on cluster health - number_of_replicas = 0 (avoid write
 * amplification) - translog.durability = async (reduce fsync frequency for faster bulk writes) -
 * translog.sync_interval = 30s (batch translog syncs) - translog.flush_threshold_size = 1024mb (JVM
 * heap-aware, if heap >= 10GB)
 *
 * <p>Refresh interval adapts to cluster health: - GREEN: -1 (disabled, max throughput) - YELLOW:
 * 60s (periodic flushes) - RED: 30s (aggressive flushes)
 *
 * <p>Guarantees restoration of original settings after reindex (success or failure).
 */
@Slf4j
public class DestinationIndexOptimizer {

  private final ESIndexBuilder indexBuilder;
  private final OpenSearchJvmInfo jvminfo;

  // Index settings constants
  private static final String INDEX_REFRESH_INTERVAL = "index.refresh_interval";
  private static final String INDEX_NUMBER_OF_REPLICAS = "index.number_of_replicas";
  private static final String INDEX_TRANSLOG_DURABILITY = "index.translog.durability";
  private static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
  private static final String INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE =
      "index.translog.flush_threshold_size";

  // Optimization tuning parameters
  private static final double MIN_HEAP_FOR_FLUSH_OPT_GB = 8.0;

  /** Original settings before optimization */
  @Data
  @Builder(toBuilder = true)
  public static class OriginalSettings {
    String refreshInterval;
    String numberOfReplicas;
    String translogDurability;
    String translogSyncInterval;
    String translogFlushThreshold;
    boolean shouldOptimizeFlushThreshold; // Track if we actually optimized it
  }

  public DestinationIndexOptimizer(ESIndexBuilder indexBuilder, OpenSearchJvmInfo jvminfo) {
    this.indexBuilder = indexBuilder;
    this.jvminfo = jvminfo;
  }

  /**
   * Optimize destination index for reindex operation with health-aware refresh intervals.
   *
   * <p>Saves original settings and applies optimizations: - refresh_interval = -1/60s/30s based on
   * cluster health (GREEN/YELLOW/RED) - number_of_replicas = 0 (avoid replicating during bulk
   * write) - translog.durability = async (reduce fsync frequency) - translog.sync_interval = 30s
   * (batch syncs) - translog.flush_threshold_size = 1024mb (JVM heap-aware, if heap >= 10GB)
   *
   * <p>Health-aware refresh intervals prevent cluster overload: - GREEN: -1 (disabled, max
   * throughput) - YELLOW: 60s (periodic flushes to reduce memory) - RED: 30s (aggressive flushes
   * for immediate relief)
   *
   * <p>Settings MUST be restored after reindex completes or fails.
   *
   * @param destIndexName Name of destination index to optimize
   * @param currentHealthState Current cluster health state (may be null, defaults to RED)
   * @param config BuildIndicesConfiguration for this run
   * @return OriginalSettings that must be passed to restoreOriginalSettings()
   * @throws IOException If unable to read or write index settings
   */
  public OriginalSettings optimizeForReindex(
      String destIndexName,
      CircuitBreakerState.HealthState currentHealthState,
      BuildIndicesConfiguration config)
      throws IOException {
    if (currentHealthState == null) {
      currentHealthState = CircuitBreakerState.HealthState.RED;
    }

    log.info(
        "Optimizing destination index {} for reindex (cluster health: {})",
        destIndexName,
        currentHealthState);

    List<String> settingsList = new ArrayList<>();
    settingsList.add(INDEX_REFRESH_INTERVAL);
    settingsList.add(INDEX_NUMBER_OF_REPLICAS);
    settingsList.add(INDEX_TRANSLOG_DURABILITY);
    settingsList.add(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE);
    settingsList.add(INDEX_TRANSLOG_SYNC_INTERVAL);

    // Step 1: Fetch all original settings in a single API call (batch operation)
    Map<String, String> originalSettingsMap =
        indexBuilder.getIndexSettings(
            destIndexName,
            INDEX_REFRESH_INTERVAL,
            INDEX_NUMBER_OF_REPLICAS,
            INDEX_TRANSLOG_DURABILITY,
            INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE,
            INDEX_TRANSLOG_SYNC_INTERVAL);

    String originalRefresh = originalSettingsMap.get(INDEX_REFRESH_INTERVAL);
    String originalReplicas = originalSettingsMap.get(INDEX_NUMBER_OF_REPLICAS);
    String originalDurability = originalSettingsMap.get(INDEX_TRANSLOG_DURABILITY);
    String originalSyncInterval =
        originalSettingsMap.getOrDefault(INDEX_TRANSLOG_SYNC_INTERVAL, null);
    String originalFlushThreshold = originalSettingsMap.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE);

    // Calculate per-node heap ONCE and determine flush threshold optimization
    boolean shouldOptimizeFlush = false;
    String optimalFlushThreshold = null;
    try {
      double minNodeHeapGb = getMinimumDataNodeHeapGB();
      optimalFlushThreshold = calculateOptimalFlushThreshold(minNodeHeapGb);
      if (optimalFlushThreshold != null && !optimalFlushThreshold.equals(originalFlushThreshold)) {
        shouldOptimizeFlush = true;
        log.debug(
            "Will optimize flush_threshold from {} to {} (min node heap: {}GB)",
            originalFlushThreshold,
            optimalFlushThreshold,
            String.format("%.1f", minNodeHeapGb));
      }
    } catch (Exception e) {
      log.warn(
          "Failed to determine optimal flush_threshold, will keep current value: {}",
          e.getMessage());
      shouldOptimizeFlush = false;
    }

    OriginalSettings original =
        OriginalSettings.builder()
            .refreshInterval(originalRefresh)
            .numberOfReplicas(originalReplicas)
            .translogDurability(originalDurability)
            .translogFlushThreshold(originalFlushThreshold)
            .shouldOptimizeFlushThreshold(shouldOptimizeFlush)
            .translogSyncInterval(originalSyncInterval)
            .build();

    log.debug(
        "Saved original settings for {}: refresh={}, replicas={}, durability={}, sync_interval={}, flush_threshold={}, shouldOptimize={}",
        destIndexName,
        originalRefresh,
        originalReplicas,
        originalDurability,
        originalSyncInterval,
        originalFlushThreshold,
        shouldOptimizeFlush);

    try {
      // Step 2: Apply optimizations in a single atomic API call (batch operation)
      log.info("Applying optimizations to {}", destIndexName);
      Settings.Builder optimizerBuilder = Settings.builder();

      // Use health-aware refresh interval based on cluster health
      String refreshInterval = currentHealthState.getRefreshInterval(config);
      optimizerBuilder.put(INDEX_REFRESH_INTERVAL, refreshInterval);
      log.debug(
          "Using {} refresh_interval for {} (health: {})",
          refreshInterval,
          destIndexName,
          currentHealthState);

      optimizerBuilder.put(INDEX_NUMBER_OF_REPLICAS, "0");
      optimizerBuilder.put(INDEX_TRANSLOG_DURABILITY, "async");
      optimizerBuilder.put(INDEX_TRANSLOG_SYNC_INTERVAL, "30s");

      // Add flush threshold optimization if it's safe for this cluster
      if (shouldOptimizeFlush) {
        optimizerBuilder.put(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, optimalFlushThreshold);
        log.debug("Added translog.flush_threshold_size={} to optimizations", optimalFlushThreshold);
      }

      Settings settings = optimizerBuilder.build();
      // Apply all optimizations in a single atomic call
      indexBuilder.updateIndexSettings(destIndexName, settings);
      log.debug("Applied {} optimizations to {} atomically", settings.size(), destIndexName);

      log.info("Successfully optimized destination index {}", destIndexName);
      return original;

    } catch (IOException | RuntimeException e) {
      // If optimization fails, try to restore original state
      // Note: updateIndexSettings wraps IOException in RuntimeException via retry mechanism
      log.error(
          "Failed to optimize destination index {}, attempting to restore original settings: {}",
          destIndexName,
          e.getMessage());
      //  Handled by the parent , incase temp indexes and other things need to be deleted
      restoreOriginalSettings(destIndexName, original);
      throw e;
    }
  }

  /**
   * Restore original index settings after reindex (success or failure).
   *
   * <p>This MUST be called in a finally block to guarantee settings are always restored. Cluster
   * will be left in broken state if this is not called.
   *
   * @param destIndexName Name of destination index
   * @param originalSettings Settings returned by optimizeForReindex()
   * @throws IOException If unable to restore settings - caller must handle to detect failures
   */
  public void restoreOriginalSettings(String destIndexName, OriginalSettings originalSettings)
      throws IOException {
    if (originalSettings == null) {
      log.debug("No original settings to restore for {}", destIndexName);
      return;
    }

    log.info("Restoring original settings for {}", destIndexName);

    Settings.Builder restoreBuilder = Settings.builder();

    // Restore refresh interval (default to "1s" if not originally set)
    String refreshInterval =
        originalSettings.getRefreshInterval() != null
            ? originalSettings.getRefreshInterval()
            : "1s";
    restoreBuilder.put(INDEX_REFRESH_INTERVAL, refreshInterval);

    // Restore number of replicas (default to "1" if not originally set)
    String numberOfReplicas =
        originalSettings.getNumberOfReplicas() != null
            ? originalSettings.getNumberOfReplicas()
            : "1";
    restoreBuilder.put(INDEX_NUMBER_OF_REPLICAS, numberOfReplicas);

    // Restore translog durability (default to "request" if not originally set)
    String durabilityValue =
        originalSettings.getTranslogDurability() != null
            ? originalSettings.getTranslogDurability()
            : "request";
    restoreBuilder.put(INDEX_TRANSLOG_DURABILITY, durabilityValue);
    // Restore translog sync interval (default to "5s" if not originally set)
    String syncIntervalValue =
        originalSettings.getTranslogSyncInterval() != null
            ? originalSettings.getTranslogSyncInterval()
            : "5s"; // ES default
    restoreBuilder.put(INDEX_TRANSLOG_SYNC_INTERVAL, syncIntervalValue);
    // Only restore flush threshold if we actually optimized it
    if (originalSettings.isShouldOptimizeFlushThreshold()
        && originalSettings.getTranslogFlushThreshold() != null) {
      restoreBuilder.put(
          INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE, originalSettings.getTranslogFlushThreshold());
    }

    // Restore all settings in a single atomic call using Settings object directly
    try {
      indexBuilder.updateIndexSettings(destIndexName, restoreBuilder.build());
      log.info("Restored original settings on {} atomically", destIndexName);
    } catch (Exception e) {
      log.error(
          "[REDINDEX-FAILURE] Failed to restore original settings on {}: {}. Cluster in non-optimal state",
          destIndexName,
          e.getMessage(),
          e);
      throw new ReindexIOException(
          "Failed to restore original settings for index " + destIndexName, e);
    }

    log.info("Completed restoration of original settings for {}", destIndexName);
  }

  /**
   * Restore minimal replicas (1) before alias swap to prevent data loss during finalization.
   *
   * <p>CRITICAL: Must be called BEFORE swapping aliases. Ensures the index has at least 1 replica
   * synced before promotion to prevent data loss if the primary shard fails during the swap window.
   *
   * @param destIndexName Index to restore replicas on
   * @param minReplicas Minimum replica count to restore to (typically 1)
   * @throws Exception If replica restoration fails
   */
  public void restoreToMinimalReplicas(String destIndexName, int minReplicas) throws Exception {
    log.info(
        "Restoring minimal replicas ({}) for {} before alias swap", minReplicas, destIndexName);
    try {
      indexBuilder.setIndexSetting(
          destIndexName, String.valueOf(minReplicas), "index.number_of_replicas");
      log.info("Successfully restored {} replica(s) on {}", minReplicas, destIndexName);
    } catch (Exception e) {
      log.error(
          "Failed to restore minimal replicas on {} - data loss risk during swap: {}",
          destIndexName,
          e.getMessage(),
          e);
      throw new ReplicaHealthException(
          "Failed to restore minimum replicas for index " + destIndexName, e);
    }
  }

  /**
   * Get the minimum JVM heap size across all data nodes (the weakest node sets the limit).
   *
   * @return Minimum heap size in GB across all data nodes, or 0 if unable to determine
   */
  private double getMinimumDataNodeHeapGB() throws IOException {
    try {
      OpenSearchJvmInfo.HeapSizeStats heapStats = jvminfo.getDataNodeHeapSizeStats();
      return heapStats.getMinHeapGB();
    } catch (Exception e) {
      log.warn("Failed to determine minimum data node heap size: {}", e.getMessage());
      throw new IOException("Cannot determine cluster heap for flush threshold optimization", e);
    }
  }

  /**
   * Calculate optimal translog flush threshold based on per-node minimum heap. Uses per-node
   * minimum (weakest node) to ensure safety across the cluster.
   *
   * <p>Safe thresholds by per-node heap: - < 8GB: null (use default, don't optimize) - 8-16GB:
   * 768mb - 16-32GB: 1024mb - 32GB+: 2048mb
   *
   * @param minNodeHeapGb Minimum JVM heap size (GB) across all data nodes
   * @return Optimal flush threshold size (e.g., "1024mb"), or null if cluster is too small for
   *     optimization
   */
  private String calculateOptimalFlushThreshold(double minNodeHeapGb) {
    if (minNodeHeapGb < MIN_HEAP_FOR_FLUSH_OPT_GB) {
      log.debug(
          "Minimum node heap ({}GB) below threshold ({}GB), skipping flush_threshold optimization",
          String.format("%.1f", minNodeHeapGb),
          String.format("%.1f", MIN_HEAP_FOR_FLUSH_OPT_GB));
      return null; // Don't optimize if cluster is too small
    } else if (minNodeHeapGb < 16.0) {
      return "768mb"; // 8-16GB per node
    } else if (minNodeHeapGb < 32.0) {
      return "1024mb"; // 16-32GB per node
    } else {
      return "2048mb"; // 32GB+ per node
    }
  }
}
