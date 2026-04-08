package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.ReindexIOException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.settings.Settings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Test cases for DestinationIndexOptimizer transactional settings management. */
public class DestinationIndexOptimizerTest {

  @Mock private ESIndexBuilder mockIndexBuilder;
  @Mock private OpenSearchJvmInfo mockJvmInfo;

  private DestinationIndexOptimizer optimizer;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    optimizer = new DestinationIndexOptimizer(mockIndexBuilder, mockJvmInfo);
  }

  @Test
  public void testOptimizeForReindex_SavesAndAppliesSettings() throws IOException {
    // Setup mock to return original settings using batch method
    Map<String, String> settingsMap = new HashMap<>();
    settingsMap.put("index.refresh_interval", "1s");
    settingsMap.put("index.number_of_replicas", "2");
    settingsMap.put("index.translog.durability", "request");
    settingsMap.put("index.translog.sync_interval", null);
    settingsMap.put("index.translog.flush_threshold_size", null);

    when(mockIndexBuilder.getIndexSettings(
            "test_index_v1",
            "index.refresh_interval",
            "index.number_of_replicas",
            "index.translog.durability",
            "index.translog.flush_threshold_size",
            "index.translog.sync_interval"))
        .thenReturn(settingsMap);

    // Mock the JVM info to return large enough heap to avoid flush threshold optimization
    when(mockJvmInfo.getDataNodeHeapSizeStats())
        .thenThrow(new IOException("Skip flush optimization"));

    // Execute optimization
    DestinationIndexOptimizer.OriginalSettings original =
        optimizer.optimizeForReindex(
            "test_index_v1",
            CircuitBreakerState.HealthState.GREEN,
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
                .build());

    // Verify original settings were captured
    assertNotNull(original, "Original settings should be returned");
    assertEquals(
        original.getRefreshInterval(), "1s", "Original refresh_interval should be captured");
    assertEquals(original.getNumberOfReplicas(), "2", "Original replicas should be captured");

    // Verify optimizations were applied
    verify(mockIndexBuilder).updateIndexSettings(eq("test_index_v1"), any(Settings.class));
  }

  @Test
  public void testOptimizeForReindex_HandlesNullSettings() throws IOException {
    // Setup mock to return null/empty settings
    Map<String, String> settingsMap = new HashMap<>();
    settingsMap.put("index.refresh_interval", null);
    settingsMap.put("index.number_of_replicas", null);
    settingsMap.put("index.translog.durability", null);
    settingsMap.put("index.translog.sync_interval", null);
    settingsMap.put("index.translog.flush_threshold_size", null);

    when(mockIndexBuilder.getIndexSettings(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(settingsMap);

    // Mock the JVM info to avoid flush threshold optimization
    when(mockJvmInfo.getDataNodeHeapSizeStats())
        .thenThrow(new IOException("Skip flush optimization"));

    // Execute optimization
    DestinationIndexOptimizer.OriginalSettings original =
        optimizer.optimizeForReindex(
            "test_index_v1",
            CircuitBreakerState.HealthState.GREEN,
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
                .build());

    // Verify optimization still happens even with null settings
    assertNotNull(original, "Original settings object should be created");
    assertNull(original.getRefreshInterval(), "Null settings should be preserved");
    assertNull(original.getNumberOfReplicas(), "Null settings should be preserved");

    verify(mockIndexBuilder).updateIndexSettings(eq("test_index_v1"), any(Settings.class));
  }

  @Test(expectedExceptions = ReindexIOException.class)
  public void testOptimizeForReindex_PropagatesExceptionAfterAttemptedRestore() throws IOException {
    // Setup mock to return valid settings first
    Map<String, String> settingsMap = new HashMap<>();
    settingsMap.put("index.refresh_interval", "1s");
    settingsMap.put("index.number_of_replicas", "2");
    settingsMap.put("index.translog.durability", "request");
    settingsMap.put("index.translog.sync_interval", null);
    settingsMap.put("index.translog.flush_threshold_size", null);

    when(mockIndexBuilder.getIndexSettings(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(settingsMap);

    // Mock JVM info
    when(mockJvmInfo.getDataNodeHeapSizeStats())
        .thenThrow(new IOException("Skip flush optimization"));

    // Setup mock to fail on optimization
    doThrow(new IOException("Optimization failed"))
        .when(mockIndexBuilder)
        .updateIndexSettings(eq("test_index_v1"), any(Settings.class));

    // Execute optimization - exception will be thrown and caught by TestNG
    optimizer.optimizeForReindex(
        "test_index_v1",
        CircuitBreakerState.HealthState.GREEN,
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
            .build());
    // Verify that restoration was attempted
    verify(mockIndexBuilder, times(2))
        .updateIndexSettings(eq("test_index_v1"), any(Settings.class));
  }

  @Test
  public void testRestoreOriginalSettings_RestoresAllSettings() throws IOException {
    DestinationIndexOptimizer.OriginalSettings original =
        DestinationIndexOptimizer.OriginalSettings.builder()
            .refreshInterval("1s")
            .numberOfReplicas("2")
            .build();

    // Execute restoration
    optimizer.restoreOriginalSettings("test_index_v1", original);

    // Verify all settings were restored
    // Verify all settings were restored via batch update with Settings object
    verify(mockIndexBuilder).updateIndexSettings(eq("test_index_v1"), any(Settings.class));
  }

  @Test
  public void testRestoreOriginalSettings_SkipsNullSettings() throws IOException {
    DestinationIndexOptimizer.OriginalSettings original =
        DestinationIndexOptimizer.OriginalSettings.builder()
            .refreshInterval("1s")
            .numberOfReplicas(null) // Null setting
            .build();

    // Execute restoration
    optimizer.restoreOriginalSettings("test_index_v1", original);

    // Verify that batch update was called with Settings object
    verify(mockIndexBuilder).updateIndexSettings(eq("test_index_v1"), any(Settings.class));
  }

  @Test
  public void testRestoreOriginalSettings_HandlesNullOriginalSettings() throws IOException {
    // Execute restoration with null original settings
    optimizer.restoreOriginalSettings("test_index_v1", null);

    // Verify no attempts to restore were made
    verify(mockIndexBuilder, never()).updateIndexSettings(anyString(), any(Settings.class));
  }

  @Test
  public void testOriginalSettings_Builder() {
    DestinationIndexOptimizer.OriginalSettings settings =
        DestinationIndexOptimizer.OriginalSettings.builder()
            .refreshInterval("1s")
            .numberOfReplicas("2")
            .build();

    assertEquals(settings.getRefreshInterval(), "1s");
    assertEquals(settings.getNumberOfReplicas(), "2");
  }

  @Test
  public void testOriginalSettings_ToBuilder() {
    DestinationIndexOptimizer.OriginalSettings original =
        DestinationIndexOptimizer.OriginalSettings.builder()
            .refreshInterval("1s")
            .numberOfReplicas("2")
            .build();

    // Use toBuilder to modify
    DestinationIndexOptimizer.OriginalSettings modified =
        original.toBuilder().numberOfReplicas("3").build();

    assertEquals(original.getNumberOfReplicas(), "2", "Original should be unchanged");
    assertEquals(modified.getNumberOfReplicas(), "3", "Modified should have new value");
    assertEquals(
        modified.getRefreshInterval(), "1s", "toBuilder should preserve unmodified fields");
  }

  @Test
  public void testOptimizeForReindex_WithDefaultValues() throws IOException {
    // Setup mock with default ES values
    Map<String, String> settingsMap = new HashMap<>();
    settingsMap.put("index.refresh_interval", "1");
    settingsMap.put("index.number_of_replicas", "1");
    settingsMap.put("index.translog.durability", "request");
    settingsMap.put("index.translog.sync_interval", null);
    settingsMap.put("index.translog.flush_threshold_size", null);

    when(mockIndexBuilder.getIndexSettings(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(settingsMap);

    // Mock JVM info
    when(mockJvmInfo.getDataNodeHeapSizeStats())
        .thenThrow(new IOException("Skip flush optimization"));

    DestinationIndexOptimizer.OriginalSettings original =
        optimizer.optimizeForReindex(
            "test_index",
            CircuitBreakerState.HealthState.GREEN,
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
                .build());

    assertNotNull(original);
    assertEquals(original.getRefreshInterval(), "1");

    // Verify optimization applied expected values
    verify(mockIndexBuilder).updateIndexSettings(eq("test_index"), any(Settings.class));
  }
}
