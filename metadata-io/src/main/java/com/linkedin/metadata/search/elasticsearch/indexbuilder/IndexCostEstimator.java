package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import java.io.IOException;
import java.util.*;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * Estimates the cost of reindexing operations for each index and classifies them into cost tiers
 * (NORMAL vs LARGE) using a cost-based scheduling formula.
 *
 * <p><b>Cost Formula:</b>
 *
 * <pre>
 * cost = (documentCount × primaryShards) / dataNodeCount
 * </pre>
 *
 * <p>This formula represents the amount of reindexing work per node. It accounts for:
 *
 * <ul>
 *   <li><b>documentCount</b>: Total documents that must be copied
 *   <li><b>primaryShards</b>: Number of primary shards (distribution width)
 *   <li><b>dataNodeCount</b>: Cluster size (capacity for parallel work)
 * </ul>
 *
 * <p><b>Tier Classification:</b>
 *
 * <ul>
 *   <li><b>NORMAL tier</b>: cost < normalIndexCostThreshold (default 1M) - Can safely run 2-3 in
 *       parallel
 *   <li><b>LARGE tier</b>: cost ≥ normalIndexCostThreshold - Should run serially (one at a time) to
 *       avoid cluster overload
 * </ul>
 *
 * <p><b>Example:</b> If an index has 1M documents, 5 primary shards, and cluster has 3 data nodes:
 *
 * <pre>
 * cost = (1_000_000 × 5) / 3 = 1,666,667
 * tier = LARGE (> 1M threshold) → serialize to one at a time
 * </pre>
 *
 * <p><b>Scheduling Impact:</b> NORMAL indices are submitted in batch mode (2-3 concurrent), LARGE
 * indices must wait for previous LARGE indices to complete, preventing cluster resource exhaustion.
 */
@Slf4j
public class IndexCostEstimator {

  private final ESIndexBuilder indexBuilder;
  private final long normalIndexCostThreshold;

  /** Cost tiers for reindex scheduling */
  public enum CostTier {
    NORMAL, // Small-medium indices, can run multiple in parallel
    LARGE // Large indices, should run serially
  }

  /** Cost information for a single index */
  @Value
  @Builder
  public static class IndexCostInfo {
    String indexName;
    long documentCount;
    int primaryShards;
    long costScore; // (docs × shards) / dataNodes
    CostTier tier;
  }

  /**
   * Constructor
   *
   * @param indexBuilder ESIndexBuilder for fetching index metadata
   * @param normalIndexCostThreshold Cost threshold to classify as NORMAL vs LARGE
   */
  public IndexCostEstimator(ESIndexBuilder indexBuilder, long normalIndexCostThreshold) {
    this.indexBuilder = indexBuilder;
    this.normalIndexCostThreshold = normalIndexCostThreshold;
  }

  /**
   * Estimate costs for multiple indices and return them sorted by cost (ascending).
   *
   * <p>Lower cost indices are returned first, allowing them to start reindexing while expensive
   * ones are still being prepared.
   *
   * @param indexNames Names of indices to cost
   * @return List of IndexCostInfo sorted by cost (low to high), with tier classification
   * @throws IOException If unable to fetch index metadata from Elasticsearch
   */
  public List<IndexCostInfo> estimateIndexCosts(List<String> indexNames) throws IOException {
    if (indexNames.isEmpty()) {
      return Collections.emptyList();
    }

    int dataNodeCount = getDataNodeCount();
    List<IndexCostInfo> costs = new ArrayList<>();
    List<String> skippedIndices = new ArrayList<>();

    for (String indexName : indexNames) {
      try {
        IndexCostInfo cost = estimateSingleIndex(indexName, dataNodeCount);
        costs.add(cost);
      } catch (Exception e) {
        log.error("Failed to estimate cost for index {}: {}", indexName, e.getMessage(), e);
        skippedIndices.add(indexName);
        // Continue with other indices instead of failing completely
      }
    }

    // Sort by cost ascending (cheapest first)
    costs.sort(Comparator.comparingLong(IndexCostInfo::getCostScore));

    if (!skippedIndices.isEmpty()) {
      log.warn(
          "Cost estimation skipped {} indices - defaulting to LARGE tier (serialized) for safety: {}",
          skippedIndices.size(),
          skippedIndices);
      // Add skipped indices as LARGE tier to ensure conservative (safe) handling
      // Unknown size = treat as potentially large and serialize to avoid cluster overload
      for (String skipped : skippedIndices) {
        IndexCostInfo skippedCost =
            IndexCostInfo.builder()
                .indexName(skipped)
                .documentCount(0) // Unknown
                .primaryShards(0) // Unknown
                .costScore(Long.MAX_VALUE) // Treat as max cost (LARGE tier)
                .tier(CostTier.LARGE) // Serialize for safety
                .build();
        costs.add(skippedCost);
      }
    }

    logCostEstimates(costs);
    return costs;
  }

  /**
   * Estimate cost for a single index
   *
   * @param indexName Name of the index
   * @param dataNodeCount Number of data nodes in cluster
   * @return IndexCostInfo with cost calculation and tier classification
   * @throws IOException If unable to fetch index metadata
   */
  private IndexCostInfo estimateSingleIndex(String indexName, int dataNodeCount)
      throws IOException {
    long documentCount = indexBuilder.getCountWithoutRefresh(indexName);
    int primaryShards = getPrimaryShardCount(indexName);

    // Cost = (documents × primary_shards) / data_nodes
    // This represents the "work per node" for reindexing
    long costScore = (documentCount * primaryShards) / Math.max(1, dataNodeCount);
    CostTier tier = costScore >= normalIndexCostThreshold ? CostTier.LARGE : CostTier.NORMAL;

    return IndexCostInfo.builder()
        .indexName(indexName)
        .documentCount(documentCount)
        .primaryShards(primaryShards)
        .costScore(costScore)
        .tier(tier)
        .build();
  }

  /**
   * Get primary shard count for an index using index settings
   *
   * @param indexName Index name
   * @return Number of primary shards
   * @throws IOException If unable to fetch index settings
   */
  private int getPrimaryShardCount(String indexName) throws IOException {
    try {
      return indexBuilder.getPrimaryShardCount(indexName);
    } catch (Exception e) {
      log.error(
          "Failed to get shard count for index {}, defaulting to 1: {}",
          indexName,
          e.getMessage(),
          e);
      return 1; // Elasticsearch 7.0+ default
    }
  }

  /**
   * Get number of data nodes in the cluster
   *
   * @return Number of data nodes capable of holding data
   */
  private int getDataNodeCount() {
    return indexBuilder.getDataNodeCount();
  }

  /** Log cost estimates for debugging and visibility */
  private void logCostEstimates(List<IndexCostInfo> costs) {
    if (costs.isEmpty()) {
      log.info("No indices to cost");
      return;
    }

    long totalCost = costs.stream().mapToLong(IndexCostInfo::getCostScore).sum();
    long normalCost =
        costs.stream()
            .filter(c -> c.getTier() == CostTier.NORMAL)
            .mapToLong(IndexCostInfo::getCostScore)
            .sum();
    long largeCost =
        costs.stream()
            .filter(c -> c.getTier() == CostTier.LARGE)
            .mapToLong(IndexCostInfo::getCostScore)
            .sum();

    log.info(
        "Index cost estimates: {} total ({} NORMAL tier, {} LARGE tier), threshold={}",
        costs.size(),
        costs.stream().filter(c -> c.getTier() == CostTier.NORMAL).count(),
        costs.stream().filter(c -> c.getTier() == CostTier.LARGE).count(),
        normalIndexCostThreshold);

    log.info(
        "Cost distribution: totalScore={}, normalScore={}, largeScore={}",
        totalCost,
        normalCost,
        largeCost);

    costs.forEach(
        cost ->
            log.debug(
                "  {} [{}]: {} docs × {} shards = cost {} (tier: {})",
                cost.getIndexName(),
                cost.getTier(),
                cost.getDocumentCount(),
                cost.getPrimaryShards(),
                cost.getCostScore(),
                cost.getTier()));
  }
}
