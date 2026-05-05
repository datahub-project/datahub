package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.util.List;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Test cases for IndexCostEstimator cost calculation and tier classification. */
public class IndexCostEstimatorTest {

  @Mock private ESIndexBuilder mockIndexBuilder;

  private IndexCostEstimator estimator;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    // Constructor takes threshold as parameter
    estimator = new IndexCostEstimator(mockIndexBuilder, 1_000_000);
  }

  @Test
  public void testEstimateIndexCosts_CalculatesCorrectly() throws IOException {
    // Cost = (documentCount * primaryShards) / dataNodes
    // Cost = (1000 * 5) / 1 = 5000
    when(mockIndexBuilder.getCountWithoutRefresh("test_index")).thenReturn(1000L);
    when(mockIndexBuilder.getPrimaryShardCount("test_index")).thenReturn(5);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("test_index"));

    assertEquals(costs.size(), 1);
    IndexCostEstimator.IndexCostInfo cost = costs.get(0);
    assertEquals(cost.getIndexName(), "test_index");
    assertEquals(cost.getCostScore(), 5000L, "Cost should be (1000 * 5) / 1 = 5000");
    assertEquals(cost.getDocumentCount(), 1000L);
    assertEquals(cost.getPrimaryShards(), 5);
  }

  @Test
  public void testEstimateIndexCosts_WithMultipleDataNodes() throws IOException {
    // Cost = (10000 * 10) / 10 = 10000
    when(mockIndexBuilder.getCountWithoutRefresh("large_index")).thenReturn(10000L);
    when(mockIndexBuilder.getPrimaryShardCount("large_index")).thenReturn(10);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(10);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("large_index"));

    assertEquals(costs.size(), 1);
    assertEquals(costs.get(0).getCostScore(), 10000L, "Cost should be (10000 * 10) / 10 = 10000");
  }

  @Test
  public void testEstimateIndexCosts_HandlesZeroDocuments() throws IOException {
    when(mockIndexBuilder.getCountWithoutRefresh("empty_index")).thenReturn(0L);
    when(mockIndexBuilder.getPrimaryShardCount("empty_index")).thenReturn(5);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("empty_index"));

    assertEquals(costs.size(), 1);
    assertEquals(costs.get(0).getCostScore(), 0L, "Cost should be 0 for empty index");
    assertEquals(
        costs.get(0).getTier(), IndexCostEstimator.CostTier.NORMAL, "Empty index should be NORMAL");
  }

  @Test
  public void testEstimateIndexCosts_HandlesException_ContinuesProcessing() throws IOException {
    // First call fails, second succeeds
    when(mockIndexBuilder.getCountWithoutRefresh("bad_index"))
        .thenThrow(new IOException("API error"));
    when(mockIndexBuilder.getCountWithoutRefresh("good_index")).thenReturn(1000L);
    when(mockIndexBuilder.getPrimaryShardCount("good_index")).thenReturn(5);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("bad_index", "good_index"));

    // Should only include good_index (bad_index is skipped on error)
    assertEquals(costs.size(), 2);
    assertEquals(costs.get(0).getIndexName(), "good_index");
  }

  @Test
  public void testTierClassification_NormalTier() throws IOException {
    // Cost just below threshold
    when(mockIndexBuilder.getCountWithoutRefresh("normal_index")).thenReturn(50000L);
    when(mockIndexBuilder.getPrimaryShardCount("normal_index")).thenReturn(10);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("normal_index"));

    assertEquals(costs.size(), 1);
    assertEquals(costs.get(0).getTier(), IndexCostEstimator.CostTier.NORMAL);
  }

  @Test
  public void testTierClassification_LargeTier() throws IOException {
    // Cost at or above threshold
    when(mockIndexBuilder.getCountWithoutRefresh("large_index")).thenReturn(1000000L);
    when(mockIndexBuilder.getPrimaryShardCount("large_index")).thenReturn(10);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("large_index"));

    assertEquals(costs.size(), 1);
    assertEquals(costs.get(0).getTier(), IndexCostEstimator.CostTier.LARGE);
  }

  @Test
  public void testEstimateIndexCosts_SortsByCostAscending() throws IOException {
    // Setup three indices with different costs
    when(mockIndexBuilder.getCountWithoutRefresh("expensive")).thenReturn(50000L);
    when(mockIndexBuilder.getPrimaryShardCount("expensive")).thenReturn(10);

    when(mockIndexBuilder.getCountWithoutRefresh("cheap")).thenReturn(1000L);
    when(mockIndexBuilder.getPrimaryShardCount("cheap")).thenReturn(5);

    when(mockIndexBuilder.getCountWithoutRefresh("medium")).thenReturn(10000L);
    when(mockIndexBuilder.getPrimaryShardCount("medium")).thenReturn(5);

    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("expensive", "cheap", "medium"));

    assertEquals(costs.size(), 3);
    // Should be sorted: cheap (5000) < medium (50000) < expensive (500000)
    assertEquals(costs.get(0).getIndexName(), "cheap");
    assertEquals(costs.get(1).getIndexName(), "medium");
    assertEquals(costs.get(2).getIndexName(), "expensive");
  }

  @Test
  public void testDefaultShardCountOnMissingIndexSetting() throws IOException {
    // When index setting is null or empty, should default to 1 (ES 7.0+ standard)
    when(mockIndexBuilder.getCountWithoutRefresh("default_shards_index")).thenReturn(10000L);
    when(mockIndexBuilder.getPrimaryShardCount("default_shards_index"))
        .thenThrow(new IOException("Index settings not found"));
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("default_shards_index"));

    assertEquals(costs.size(), 1);
    // Should use default of 1 shard (ES 7.0+ standard): (10000 * 1) / 1 = 10000
    assertEquals(costs.get(0).getCostScore(), 10000L);
    assertEquals(costs.get(0).getPrimaryShards(), 1);
  }

  @Test
  public void testDataNodeCountAffectsCostCalculation() throws IOException {
    // Same index with different data node counts
    when(mockIndexBuilder.getCountWithoutRefresh("test_index")).thenReturn(100000L);
    when(mockIndexBuilder.getPrimaryShardCount("test_index")).thenReturn(10);
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(10);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("test_index"));

    assertEquals(costs.size(), 1);
    // Cost = (100000 * 10) / 10 = 100000
    assertEquals(costs.get(0).getCostScore(), 100000L);
  }

  @Test
  public void testEmptyIndexList() throws IOException {
    List<IndexCostEstimator.IndexCostInfo> costs = estimator.estimateIndexCosts(List.of());

    assertEquals(costs.size(), 0);
  }

  @Test
  public void testMultipleIndicesWithMixedErrors() throws IOException {
    // Setup: some succeed, some fail, some have unusual settings
    when(mockIndexBuilder.getCountWithoutRefresh("index1")).thenReturn(1000L);
    when(mockIndexBuilder.getPrimaryShardCount("index1")).thenReturn(5);

    when(mockIndexBuilder.getCountWithoutRefresh("index2"))
        .thenThrow(new IOException("Connection error"));

    when(mockIndexBuilder.getCountWithoutRefresh("index3")).thenReturn(5000L);
    when(mockIndexBuilder.getPrimaryShardCount("index3"))
        .thenThrow(new IOException("Invalid shard count"));

    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    List<IndexCostEstimator.IndexCostInfo> costs =
        estimator.estimateIndexCosts(List.of("index1", "index2", "index3"));

    assertEquals(costs.size(), 3);
    assertEquals(costs.get(0).getIndexName(), "index1");
    assertEquals(costs.get(1).getIndexName(), "index3");
    // index3 should use default shard count of 1 (ES 7.0+ standard) due to invalid setting
    assertEquals(costs.get(1).getPrimaryShards(), 1);
  }
}
