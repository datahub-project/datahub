package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParallelReindexOrchestratorTest {

  @Mock private ESIndexBuilder mockIndexBuilder;

  @Mock private OpenSearchJvmInfo jvmInfo;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  /** Setup all default mocks with sensible values. Can be overridden per-test. */
  private void setupDefaultMocks() throws Exception {
    when(mockIndexBuilder.getJvminfo()).thenReturn(jvmInfo);
    when(jvmInfo.getDataNodeHeapSizeStats())
        .thenReturn(new OpenSearchJvmInfo.HeapSizeStats(0L, 0L, 0.0, 0L, 0));
    when(mockIndexBuilder.getClusterHealth())
        .thenReturn(
            new ClusterHealthResponse("test-cluster", new String[] {}, ClusterState.EMPTY_STATE));

    when(mockIndexBuilder.getIndexSetting(anyString(), anyString())).thenReturn("1");
    when(mockIndexBuilder.getIndexSetting(anyString(), eq("index.refresh_interval")))
        .thenReturn("1s");
    when(mockIndexBuilder.getIndexSetting(anyString(), eq("index.number_of_replicas")))
        .thenReturn("1");
    when(mockIndexBuilder.getIndexSetting(anyString(), eq("index.number_of_shards")))
        .thenReturn("1");
    when(mockIndexBuilder.getIndexSetting(anyString(), eq("index.translog.durability")))
        .thenReturn("request");
    when(mockIndexBuilder.getIndexSetting(anyString(), eq("index.translog.sync_interval")))
        .thenReturn("5s");

    when(mockIndexBuilder.getCountWithoutRefresh(anyString())).thenReturn(1000L);
    when(mockIndexBuilder.getCount(anyString())).thenReturn(1000L);

    doNothing().when(mockIndexBuilder).setIndexSetting(anyString(), anyString(), anyString());

    doNothing().when(mockIndexBuilder).createIndex(anyString(), any());

    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);

    doNothing().when(mockIndexBuilder).waitForIndexGreenHealth(anyString(), anyInt());

    doNothing().when(mockIndexBuilder).swapAliases(anyString(), anyString(), anyString());

    doNothing().when(mockIndexBuilder).deleteIndex(anyString());

    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:default2"));

    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenReturn(
            new ESIndexBuilder.TaskStatusResult(Collections.emptyMap(), Collections.emptySet()));
  }

  private ParallelReindexOrchestrator createOrchestratorWithMocks() throws Exception {
    setupDefaultMocks();

    // Create config for testing with small concurrency limits and fast intervals
    BuildIndicesConfiguration config =
        BuildIndicesConfiguration.builder()
            .enableParallelReindex(true)
            .maxConcurrentNormalReindex(2)
            .maxConcurrentLargeReindex(1)
            .taskCheckIntervalSeconds(1)
            .maxReindexHours(1)
            .docCountValidationRetryCount(1)
            .docCountValidationRetrySleepMs(1)
            .maxConcurrentFinalizations(4)
            .replicaSyncTimeoutMinutes(1)
            .yellowStabilitySeconds(5)
            .greenStabilitySeconds(5)
            .redRecoverySeconds(10)
            .build();

    CircuitBreakerState circuitBreakerState = new CircuitBreakerState(config);
    return new ParallelReindexOrchestrator(mockIndexBuilder, config, circuitBreakerState);
  }

  @Test
  public void testReindexAll_SkipsNonReindexConfigs() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();

    // Create config that doesn't require reindex
    ReindexConfig config =
        ReindexConfig.builder()
            .name("test_index")
            .exists(true)
            .currentSettings(Settings.EMPTY)
            .targetSettings(Collections.emptyMap())
            .currentMappings(Collections.emptyMap())
            .targetMappings(Collections.emptyMap())
            .enableIndexMappingsReindex(false)
            .enableIndexSettingsReindex(false)
            .enableStructuredPropertiesReindex(false)
            .version("1.0")
            .build();

    // Config with requiresReindex=false should be filtered out
    assertFalse(config.requiresReindex(), "Test config should not require reindex");

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config));

    assertTrue(
        results.isEmpty(), "Non-reindex configs should be filtered and return empty results");
    verifyNoInteractions(mockIndexBuilder);
  }

  @Test
  public void testReindexAll_HandlesEmptyIndex() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();

    // Create config that requires reindex
    ReindexConfig config = createReindexConfig("empty_index");

    // Mock empty index (0 documents) for both calls: getCountWithoutRefresh and getCount
    when(mockIndexBuilder.getCountWithoutRefresh("empty_index")).thenReturn(0L);
    when(mockIndexBuilder.getCount("empty_index")).thenReturn(0L);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config));

    assertEquals(results.size(), 1);
    assertEquals(
        results.get("empty_index"),
        ReindexResult.REINDEXED_SKIPPED_0DOCS,
        "Empty index should be skipped");

    // Verify createIndex and deleteIndex were NOT called (empty index skips reindex)
    verify(mockIndexBuilder, times(0)).createIndex(anyString(), any());
    verify(mockIndexBuilder, times(0)).deleteIndex(anyString());
  }

  // Helper to create a config that requires reindexing
  private ReindexConfig createReindexConfig(String indexName) {
    return ReindexConfig.builder()
        .name(indexName)
        .exists(true)
        .currentSettings(Settings.EMPTY)
        .targetSettings(
            Map.of(
                "index",
                Map.of("number_of_shards", "2"))) // Different from default, triggers reindex
        .currentMappings(Map.of("properties", Map.of("field1", Map.of("type", "text"))))
        .targetMappings(
            Map.of(
                "properties",
                Map.of(
                    "field1",
                    Map.of("type", "text"),
                    "field2",
                    Map.of("type", "keyword")))) // New field, triggers reindex
        .enableIndexMappingsReindex(true)
        .enableIndexSettingsReindex(true)
        .enableStructuredPropertiesReindex(false)
        .version("1.0")
        .build();
  }

  // Helper to mock all necessary finalization calls
  private void mockFinalizationCalls(String tempIndexName) throws Exception {
    // Mock getIndexSetting calls during optimization
    when(mockIndexBuilder.getIndexSetting(tempIndexName, anyString()))
        .thenReturn(null); // Simulate no prior settings
    when(mockIndexBuilder.getIndexSetting(anyString(), "index.number_of_shards")).thenReturn("1");
    when(mockIndexBuilder.getIndexSetting(anyString(), "index.number_of_replicas")).thenReturn("1");

    // Mock setIndexSetting calls during optimization and restoration
    doNothing().when(mockIndexBuilder).setIndexSetting(anyString(), anyString(), anyString());

    // Mock replica restoration
    doNothing()
        .when(mockIndexBuilder)
        .setIndexSetting(tempIndexName, "1", "index.number_of_replicas");

    // Mock health check
    doNothing().when(mockIndexBuilder).waitForIndexGreenHealth(tempIndexName, 300);

    // Mock getDataNodeCount for heap calculation
    when(mockIndexBuilder.getDataNodeCount()).thenReturn(1);
  }

  @Test
  public void testReindexAll_SuccessfulSingleIndex() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();
    ReindexConfig config = createReindexConfig("test_index");

    // Override default mocks for this test
    when(mockIndexBuilder.getCountWithoutRefresh("test_index")).thenReturn(1000L);
    when(mockIndexBuilder.getCount("test_index")).thenReturn(1000L);

    // Mock successful task submission
    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:12345"));

    // Mock task completion
    GetTaskResponse mockCompletedTask = mock(GetTaskResponse.class);
    when(mockCompletedTask.isCompleted()).thenReturn(true);
    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenReturn(
            new ESIndexBuilder.TaskStatusResult(
                Map.of("node1:12345", mockCompletedTask), Collections.emptySet()));

    // Mock temp index document count to match source
    when(mockIndexBuilder.getCount(contains("test_index_"))).thenReturn(1000L);

    // Wait briefly for async finalization to complete
    Thread.sleep(100);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config));

    // Wait for finalization to complete
    Thread.sleep(200);

    assertEquals(results.size(), 1, "Should have result for test_index");
    assertEquals(
        results.get("test_index"), ReindexResult.REINDEXED, "test_index should be reindexed");

    verify(mockIndexBuilder, atLeastOnce())
        .submitReindexInternal(any(), anyString(), any(), eq(Float.POSITIVE_INFINITY));
    verify(mockIndexBuilder, atLeastOnce()).swapAliases(eq("test_index"), eq(null), anyString());
  }

  @Test
  public void testReindexAll_SuccessfulParallelReindex() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();

    ReindexConfig config1 = createReindexConfig("index1");
    ReindexConfig config2 = createReindexConfig("index2");

    // Mock both indices with documents
    when(mockIndexBuilder.getCountWithoutRefresh("index1")).thenReturn(1000L);
    when(mockIndexBuilder.getCountWithoutRefresh("index2")).thenReturn(1000L);
    when(mockIndexBuilder.getCount("index1")).thenReturn(1000L);
    when(mockIndexBuilder.getCount("index2")).thenReturn(1000L);

    // Mock task submissions with different task IDs
    when(mockIndexBuilder.submitReindexInternal(
            any(),
            eq(ESIndexBuilder.getNextIndexName("index1", Long.MAX_VALUE - 1000)),
            any(),
            eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:11111"));
    when(mockIndexBuilder.submitReindexInternal(
            any(),
            eq(ESIndexBuilder.getNextIndexName("index2", Long.MAX_VALUE - 500)),
            any(),
            eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:22222"));
    // Fallback to return different task IDs for any other submission
    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenAnswer(
            invocation -> {
              String tempIndexName = invocation.getArgument(1);
              if (tempIndexName.contains("index1")) {
                return Map.of("taskId", "node1:11111");
              } else {
                return Map.of("taskId", "node1:22222");
              }
            });

    // Mock task completion
    GetTaskResponse mockTask1 = mock(GetTaskResponse.class);
    GetTaskResponse mockTask2 = mock(GetTaskResponse.class);

    AtomicInteger count = new AtomicInteger();
    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenAnswer(
            invocation -> {
              java.util.Set<String> taskIds = invocation.getArgument(0);
              Map<String, GetTaskResponse> responses = new HashMap<>();
              if (count.get() > 2) {
                when(mockTask1.isCompleted()).thenReturn(true);
              }
              if (count.get() > 4) {
                when(mockTask2.isCompleted()).thenReturn(true);
              }
              for (String taskId : taskIds) {
                if (taskId.equals("node1:11111")) {
                  responses.put(taskId, mockTask1);
                } else if (taskId.equals("node1:22222")) {
                  responses.put(taskId, mockTask2);
                }
              }
              count.getAndIncrement();
              return new ESIndexBuilder.TaskStatusResult(responses, Collections.emptySet());
            });

    // Mock temp index counts
    when(mockIndexBuilder.getCount(contains("index1_"))).thenReturn(1000L);
    when(mockIndexBuilder.getCount(contains("index2_"))).thenReturn(1000L);

    // Wait for async finalization
    Thread.sleep(100);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config1, config2));

    // Wait for finalization to complete
    Thread.sleep(300);

    assertEquals(results.size(), 2, "Should have results for both indices");
    assertEquals(results.get("index1"), ReindexResult.REINDEXED, "index1 should be reindexed");
    assertEquals(results.get("index2"), ReindexResult.REINDEXED, "index2 should be reindexed");
  }

  @Test
  public void testTimeoutHandling_ChecksTimeoutCondition() throws Exception {
    // Test that tasks that complete immediately don't trigger timeout
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();
    ReindexConfig config = createReindexConfig("timeout_test");

    when(mockIndexBuilder.getCountWithoutRefresh("timeout_test")).thenReturn(1000L);
    when(mockIndexBuilder.getCount("timeout_test")).thenReturn(1000L);
    when(mockIndexBuilder.getCount(contains("timeout_test_"))).thenReturn(1000L);

    // Mock task submission
    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:88888"));

    // Task completes immediately - no timeout
    GetTaskResponse completedTask = mock(GetTaskResponse.class);
    when(completedTask.isCompleted()).thenReturn(true);
    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenReturn(
            new ESIndexBuilder.TaskStatusResult(
                Map.of("node1:88888", completedTask), Collections.emptySet()));

    // Wait for async finalization
    Thread.sleep(100);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config));

    // Wait for finalization to complete
    Thread.sleep(200);

    // Should succeed without timeout since task completes
    assertEquals(
        results.get("timeout_test"),
        ReindexResult.REINDEXED,
        "Task should complete without timeout");
  }

  @Test
  public void testDocumentCountValidation_Mismatch() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();
    ReindexConfig config = createReindexConfig("mismatch_test");

    when(mockIndexBuilder.getCountWithoutRefresh("mismatch_test")).thenReturn(10000L);
    when(mockIndexBuilder.getCount("mismatch_test")).thenReturn(10000L);

    // Mock task submission
    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenReturn(Map.of("taskId", "node1:66666"));

    // Task completes
    GetTaskResponse completedTask = mock(GetTaskResponse.class);
    when(completedTask.isCompleted()).thenReturn(true);
    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenReturn(
            new ESIndexBuilder.TaskStatusResult(
                Map.of("node1:66666", completedTask), Collections.emptySet()));

    when(mockIndexBuilder.getCount(contains("mismatch_test_"))).thenReturn(9000L);

    // Wait for async finalization
    Thread.sleep(100);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config));

    // Wait for finalization to complete
    Thread.sleep(200);

    assertEquals(
        results.get("mismatch_test"),
        ReindexResult.FAILED_DOC_COUNT_MISMATCH,
        "Should fail due to document count mismatch");
    verify(mockIndexBuilder, atLeastOnce()).deleteIndex(contains("mismatch_test_"));
  }

  @Test
  public void testPartialFailure_MixedSuccessAndDocCountMismatch() throws Exception {
    ParallelReindexOrchestrator orchestrator = createOrchestratorWithMocks();
    ReindexConfig config1 = createReindexConfig("success_index");
    ReindexConfig config2 = createReindexConfig("failure_index");

    // Both indices have documents
    when(mockIndexBuilder.getCountWithoutRefresh("success_index")).thenReturn(1000L);
    when(mockIndexBuilder.getCountWithoutRefresh("failure_index")).thenReturn(5000L);
    when(mockIndexBuilder.getCount("success_index")).thenReturn(1000L);
    when(mockIndexBuilder.getCount("failure_index")).thenReturn(5000L);

    // Mock task submissions with different task IDs
    when(mockIndexBuilder.submitReindexInternal(
            any(), anyString(), any(), eq(Float.POSITIVE_INFINITY)))
        .thenAnswer(
            invocation -> {
              String tempIndexName = invocation.getArgument(1);
              if (tempIndexName.contains("success_index")) {
                return Map.of("taskId", "node1:11111");
              } else {
                return Map.of("taskId", "node1:22222");
              }
            });

    // Mock task completion
    GetTaskResponse task1 = mock(GetTaskResponse.class);
    GetTaskResponse task2 = mock(GetTaskResponse.class);

    AtomicInteger count = new AtomicInteger();
    when(mockIndexBuilder.getTaskStatusMultiple(any()))
        .thenAnswer(
            invocation -> {
              Set<String> taskIds = invocation.getArgument(0);
              Map<String, GetTaskResponse> responses = new HashMap<>();
              if (count.get() > 3) {
                when(task1.isCompleted()).thenReturn(true);
              }
              if (count.get() > 5) {
                when(task2.isCompleted()).thenReturn(true);
              }
              for (String taskId : taskIds) {
                if (taskId.equals("node1:11111")) {
                  responses.put(taskId, task1);
                } else if (taskId.equals("node1:22222")) {
                  responses.put(taskId, task2);
                }
              }
              count.getAndIncrement();
              return new ESIndexBuilder.TaskStatusResult(responses, Collections.emptySet());
            });

    // success_index: doc count matches (1000 == 1000)
    // failure_index: doc count mismatch (5000 != 4500, diff > tolerance)
    when(mockIndexBuilder.getCount(contains("success_index_"))).thenReturn(1000L);
    when(mockIndexBuilder.getCount(contains("failure_index_"))).thenReturn(4500L);

    // Wait for async finalization
    Thread.sleep(100);

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(config1, config2));

    // Wait for finalization to complete
    Thread.sleep(300);

    // Verify results: 1 success, 1 failure
    assertEquals(results.size(), 2, "Should have results for both indices");
    assertEquals(
        results.get("success_index"), ReindexResult.REINDEXED, "success_index should be reindexed");
    assertEquals(
        results.get("failure_index"),
        ReindexResult.FAILED_DOC_COUNT_MISMATCH,
        "failure_index should fail due to doc count mismatch");

    // Verify cleanup happened for failed index
    verify(mockIndexBuilder, atLeastOnce()).deleteIndex(contains("failure_index_"));

    // Verify alias swap only happened for successful index
    verify(mockIndexBuilder, atLeastOnce()).swapAliases(eq("success_index"), eq(null), anyString());
  }

  @Test
  void testReindexSubmission_ThrowsIOException() throws Exception {
    setupDefaultMocks();

    // Mock submitReindexInternal to throw IOException (IO error during submission)
    when(mockIndexBuilder.submitReindexInternal(
            any(String[].class),
            anyString(),
            any(ReindexConfig.class),
            eq(Float.POSITIVE_INFINITY)))
        .thenThrow(new IOException("Network timeout during reindex submission"));

    BuildIndicesConfiguration config =
        BuildIndicesConfiguration.builder().enableParallelReindex(true).build();

    ReindexConfig reindexConfig = createReindexConfig("test_index");

    ParallelReindexOrchestrator orchestrator =
        new ParallelReindexOrchestrator(mockIndexBuilder, config, new CircuitBreakerState(config));

    Map<String, ReindexResult> results = orchestrator.reindexAll(List.of(reindexConfig));

    // Verify that IO exception during submission results in FAILED_SUBMISSION_IO
    assertEquals(
        results.get("test_index"),
        ReindexResult.FAILED_SUBMISSION_IO,
        "IO error during submission should result in FAILED_SUBMISSION_IO");

    // Verify temp index was cleaned up
    verify(mockIndexBuilder, atLeastOnce()).deleteIndex(contains("test_index_"));
  }
}
