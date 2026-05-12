package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder.IncrementalReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder.PollReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BuildIndicesIncrementalStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String NEXT_INDEX_NAME = "datasetindex_v2_0_14_0-0_1679000000000";

  @Mock private EntityService<?> entityService;
  @Mock private ESIndexBuilder indexBuilder;
  @Mock private ElasticSearchIndexed indexedService;
  @Mock private UpgradeContext upgradeContext;
  @Mock private Upgrade upgrade;

  private OperationContext opContext;
  private BuildIndicesIncrementalStep step;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    IndexUtils.clearReindexConfigCache();
    opContext = TestOperationContexts.systemContextNoValidate();

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    when(entityService.getLatestEnvelopedAspect(any(), any(), any(), any())).thenReturn(null);
    when(entityService.ingestProposal(any(), any(), any(), anyBoolean()))
        .thenReturn(mock(IngestResult.class));

    // Default: service returns the index builder for our test index
    ReindexConfig reindexConfig = mockReindexConfig(INDEX_NAME, true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(reindexConfig));
    when(indexedService.getIndexBuilder()).thenReturn(indexBuilder);
    when(indexBuilder.getBackingIndices(anyString())).thenReturn(Set.of("datasetindex_v2_old"));
    when(indexBuilder.validateAndSwapAlias(anyString(), anyString())).thenReturn(true);

    step =
        new BuildIndicesIncrementalStep(
            opContext, List.of(indexedService), Set.of(), entityService, UPGRADE_VERSION);
  }

  @Test
  public void testIdIncludesVersion() {
    assertEquals(step.id(), "BuildIndicesIncremental_0.14.0-0");
  }

  @Test
  public void testSucceedsWhenNoIndicesNeedReindex() throws Throwable {
    // No indices need reindex — return empty list so getIndicesNeedingReindex returns empty
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of());

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).buildIndexIncremental(any(), anyString());
  }

  @Test
  public void testFreshStartSuccessful() throws Throwable {
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);

    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(100L, 100L));
    when(indexBuilder.pollReindexCompletion(
            eq(INDEX_NAME), eq(NEXT_INDEX_NAME), any(), anyInt(), anyMap(), eq("task1")))
        .thenReturn(pollResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).buildIndexIncremental(any(), eq(UPGRADE_VERSION));
    verify(indexBuilder)
        .pollReindexCompletion(any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder).undoReindexOptimalSettings(eq(NEXT_INDEX_NAME), any(), anyMap());
    verify(indexBuilder).validateAndSwapAlias(eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSkippedEmptyIndex() throws Throwable {
    IncrementalReindexResult emptyResult =
        new IncrementalReindexResult(NEXT_INDEX_NAME, 1679000000000L, null, true, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(any(), eq(UPGRADE_VERSION))).thenReturn(emptyResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should not poll or undo settings for empty index
    verify(indexBuilder, never())
        .pollReindexCompletion(any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder, never()).undoReindexOptimalSettings(any(), any(), anyMap());
  }

  @Test
  public void testPollTimeoutReturnsFailed() throws Throwable {
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);

    PollReindexResult timedOut = new PollReindexResult(false, Map.of(), Pair.of(100L, 50L));
    when(indexBuilder.pollReindexCompletion(any(), any(), any(), anyInt(), anyMap(), anyString()))
        .thenReturn(timedOut);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never()).undoReindexOptimalSettings(any(), any(), anyMap());
  }

  @Test
  public void testResumesInProgressFromPreviousRun() throws Throwable {
    // Simulate previous state with IN_PROGRESS for our index
    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            null,
            1679000000000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);

    DataHubUpgradeResult upgradeResult = mock(DataHubUpgradeResult.class);
    when(upgradeResult.getResult()).thenReturn(new StringMap(previousState));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(upgradeResult));

    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(100L, 100L));
    when(indexBuilder.pollReindexCompletion(
            eq(INDEX_NAME), eq(NEXT_INDEX_NAME), any(), anyInt(), anyMap(), eq("")))
        .thenReturn(pollResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should NOT call buildIndexIncremental — just resume polling
    verify(indexBuilder, never()).buildIndexIncremental(any(), anyString());
    verify(indexBuilder)
        .pollReindexCompletion(any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder).undoReindexOptimalSettings(eq(NEXT_INDEX_NAME), any(), anyMap());
    verify(indexBuilder).validateAndSwapAlias(eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSkipsCompletedFromPreviousRun() throws Throwable {
    // Simulate previous state with COMPLETED for our index
    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            null,
            1679000000000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);

    DataHubUpgradeResult upgradeResult = mock(DataHubUpgradeResult.class);
    when(upgradeResult.getResult()).thenReturn(new StringMap(previousState));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(upgradeResult));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should not build or poll — index was already done
    verify(indexBuilder, never()).buildIndexIncremental(any(), anyString());
    verify(indexBuilder, never())
        .pollReindexCompletion(any(), any(), any(), anyInt(), anyMap(), anyString());
  }

  @Test
  public void testFailsWhenNoIndexBuilderFound() throws Throwable {
    // Return a reindex config for an index that no service provides a builder for
    ReindexConfig unknownConfig = mockReindexConfig("unknown_index", true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(unknownConfig));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testExceptionReturnsFailed() throws Throwable {
    when(indexBuilder.buildIndexIncremental(any(), anyString()))
        .thenThrow(new RuntimeException("ES connection error"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  private static ReindexConfig mockReindexConfig(String name, boolean requiresReindex) {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.name()).thenReturn(name);
    when(config.requiresReindex()).thenReturn(requiresReindex);
    when(config.isSettingsReindex()).thenReturn(false);
    when(config.isPureMappingsAddition()).thenReturn(false);
    when(config.requiresApplyMappings()).thenReturn(false);
    when(config.requiresApplySettings()).thenReturn(false);
    when(config.requiresDataBackfill()).thenReturn(requiresReindex);
    when(config.targetSettings())
        .thenReturn(
            ImmutableMap.of(
                "index",
                ImmutableMap.of(
                    "number_of_shards", 1, "number_of_replicas", 1, "refresh_interval", "1s")));
    return config;
  }
}
