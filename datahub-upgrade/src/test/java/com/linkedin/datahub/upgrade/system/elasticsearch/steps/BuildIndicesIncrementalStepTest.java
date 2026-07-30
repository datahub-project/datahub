package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentCaptor;
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
    when(indexBuilder.getBackingIndices(any(OperationContext.class), anyString()))
        .thenReturn(Set.of("datasetindex_v2_old"));
    when(indexBuilder.validateAndSwapAlias(any(OperationContext.class), anyString(), anyString()))
        .thenReturn(true);
    when(indexBuilder.indexExists(any(OperationContext.class), anyString())).thenReturn(true);

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
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }

  @Test
  public void testFreshStartSuccessful() throws Throwable {
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);

    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(100L, 100L));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            eq("task1")))
        .thenReturn(pollResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .buildIndexIncremental(any(OperationContext.class), any(), eq(UPGRADE_VERSION));
    verify(indexBuilder)
        .pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder)
        .undoReindexOptimalSettings(
            any(OperationContext.class), eq(NEXT_INDEX_NAME), any(ReindexConfig.class), anyMap());
    verify(indexBuilder)
        .validateAndSwapAlias(any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSkippedEmptyIndex() throws Throwable {
    IncrementalReindexResult emptyResult =
        new IncrementalReindexResult(NEXT_INDEX_NAME, 1679000000000L, null, true, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(emptyResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should not poll or undo settings for empty index
    verify(indexBuilder, never())
        .pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder, never())
        .undoReindexOptimalSettings(
            any(OperationContext.class), any(String.class), any(ReindexConfig.class), anyMap());
  }

  @Test
  public void testNonExistingIndexIsCreated() throws Throwable {
    // Fresh-install scenario: the index has never been created. The broadened
    // getIndicesNeedingReindexOrBuild filter picks it up, and BuildIndicesIncrementalStep
    // must delegate to buildIndex (which calls createIndex under the canonical name) rather
    // than the incremental path — getSourceDocCount / getBackingIndices / validateAndSwapAlias
    // all throw on a missing alias.
    ReindexConfig newIndexConfig = mockReindexConfig(INDEX_NAME, false);
    when(newIndexConfig.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(newIndexConfig));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Critical: the non-existing index must be created via buildIndex
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(newIndexConfig));
    // None of the incremental-path operations should run — they all assume a pre-existing source
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder, never()).getBackingIndices(any(OperationContext.class), anyString());
    verify(indexBuilder, never())
        .pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder, never())
        .validateAndSwapAlias(any(OperationContext.class), anyString(), anyString());
  }

  @Test
  public void testNonExistingIndexBuildThrowsReturnsFailed() throws Throwable {
    // If the fresh-create path fails (e.g. ES is down), the step must propagate FAILED
    // rather than continue past and pretend the index is ready.
    ReindexConfig newIndexConfig = mockReindexConfig(INDEX_NAME, false);
    when(newIndexConfig.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(newIndexConfig));
    doThrow(new IOException("ES unavailable"))
        .when(indexBuilder)
        .buildIndex(any(OperationContext.class), eq(newIndexConfig));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(newIndexConfig));
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }

  @Test
  public void testPollTimeoutReturnsFailed() throws Throwable {
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);

    PollReindexResult timedOut = new PollReindexResult(false, Map.of(), Pair.of(100L, 50L));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString()))
        .thenReturn(timedOut);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never())
        .undoReindexOptimalSettings(
            any(OperationContext.class), any(String.class), any(ReindexConfig.class), anyMap());
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
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            eq("")))
        .thenReturn(pollResult);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should NOT call buildIndexIncremental — just resume polling
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder)
        .pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString());
    verify(indexBuilder)
        .undoReindexOptimalSettings(
            any(OperationContext.class), eq(NEXT_INDEX_NAME), any(ReindexConfig.class), anyMap());
    verify(indexBuilder)
        .validateAndSwapAlias(any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testResumeRestartsFromScratchWhenTargetIndexMissing() throws Throwable {
    // Simulate previous state with IN_PROGRESS, but the target index no longer exists in ES.
    // This happens when an instance is paused for an extended period and ES cleanup removes
    // the partially-populated target index. The step should fall through to the fresh-start
    // path instead of throwing index_not_found_exception in pollReindexCompletion.
    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            "datasetindex_v2_old",
            1679000000000L,
            500L,
            "task-abc",
            true,
            IncrementalReindexState.Status.IN_PROGRESS);

    DataHubUpgradeResult upgradeResult = mock(DataHubUpgradeResult.class);
    when(upgradeResult.getResult()).thenReturn(new StringMap(previousState));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(upgradeResult));

    // Target index is missing
    when(indexBuilder.indexExists(any(OperationContext.class), eq(NEXT_INDEX_NAME)))
        .thenReturn(false);

    // Wire up the fresh-start path that the code should fall through to
    String freshNextIndex = "datasetindex_v2_0_14_0-0_1679999999999";
    IncrementalReindexResult freshResult =
        new IncrementalReindexResult(
            freshNextIndex, 1679999999999L, "task-fresh", false, 2, 500L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(freshResult);

    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(500L, 500L));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(freshNextIndex),
            any(),
            anyInt(),
            anyMap(),
            eq("task-fresh")))
        .thenReturn(pollResult);
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(freshNextIndex)))
        .thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    // Should NOT have attempted to resume polling on the missing index
    verify(indexBuilder, never())
        .pollReindexCompletion(
            any(OperationContext.class),
            any(),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString());
    // Should have started fresh
    verify(indexBuilder)
        .buildIndexIncremental(any(OperationContext.class), any(), eq(UPGRADE_VERSION));
    verify(indexBuilder)
        .validateAndSwapAlias(any(OperationContext.class), eq(INDEX_NAME), eq(freshNextIndex));
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
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder, never())
        .pollReindexCompletion(
            any(OperationContext.class), any(), any(), any(), anyInt(), anyMap(), anyString());
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
    when(indexBuilder.buildIndexIncremental(any(OperationContext.class), any(), anyString()))
        .thenThrow(new RuntimeException("ES connection error"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSwapFailurePersistsInProgressNotCompleted() throws Throwable {
    // Regression: a reindex that completes but whose alias swap fails (doc-count mismatch) must be
    // persisted as IN_PROGRESS, NOT COMPLETED. If it were marked COMPLETED, a rerun would hit the
    // "already COMPLETED, skipping" branch and silently succeed while the alias still points at the
    // stale index. Keeping it IN_PROGRESS lets the rerun resume and retry the swap.
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);

    // Reindex (data copy) completes...
    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(100L, 90L));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString()))
        .thenReturn(pollResult);
    // ...but the alias swap fails (e.g. doc-count mismatch on a live, high-write index).
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME)))
        .thenReturn(false);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    // The run must fail loudly, not silently succeed.
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    // The last persisted checkpoint must record IN_PROGRESS (not COMPLETED), with the next index
    // name retained so a resumed run can re-poll and retry the swap.
    Map<String, String> persisted = captureLastPersistedState();
    assertEquals(
        IncrementalReindexState.getStatus(persisted, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.IN_PROGRESS));
    assertEquals(
        IncrementalReindexState.get(persisted, INDEX_NAME, IncrementalReindexState.NEXT_INDEX_NAME),
        Optional.of(NEXT_INDEX_NAME));
  }

  @Test
  public void testRerunAfterSwapFailureRetriesSwapInsteadOfSkipping() throws Throwable {
    // End-to-end: run 1 reindexes then fails the swap; run 2 (fed run 1's persisted state) must
    // resume and RETRY the swap — not skip the index as "already COMPLETED".
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    PollReindexResult pollResult = new PollReindexResult(true, Map.of(), Pair.of(100L, 90L));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString()))
        .thenReturn(pollResult);

    // Run 1: swap fails.
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME)))
        .thenReturn(false);
    UpgradeStepResult run1 = step.executable().apply(upgradeContext);
    assertEquals(run1.result(), DataHubUpgradeState.FAILED);

    // Feed run 1's persisted state back in as the previous state for run 2.
    Map<String, String> stateAfterRun1 = captureLastPersistedState();
    DataHubUpgradeResult previousResult = mock(DataHubUpgradeResult.class);
    when(previousResult.getResult()).thenReturn(new StringMap(stateAfterRun1));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    // Run 2: swap now succeeds.
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME)))
        .thenReturn(true);
    UpgradeStepResult run2 = step.executable().apply(upgradeContext);

    assertEquals(run2.result(), DataHubUpgradeState.SUCCEEDED);
    // Run 2 must have resumed (re-polled + retried the swap), NOT re-run the reindex from scratch,
    // and NOT skipped the index.
    verify(indexBuilder, times(1))
        .buildIndexIncremental(any(OperationContext.class), any(), eq(UPGRADE_VERSION));
    verify(indexBuilder, times(2))
        .validateAndSwapAlias(any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
  }

  private Map<String, String> captureLastPersistedState() {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(OperationContext.class), captor.capture(), any(), anyBoolean());
    List<MetadataChangeProposal> proposals = captor.getAllValues();
    MetadataChangeProposal last = proposals.get(proposals.size() - 1);
    DataHubUpgradeResult decoded =
        GenericRecordUtils.deserializeAspect(
            last.getAspect().getValue(),
            last.getAspect().getContentType(),
            DataHubUpgradeResult.class);
    return decoded.getResult() == null ? Map.of() : decoded.getResult();
  }

  private static ReindexConfig mockReindexConfig(String name, boolean requiresReindex) {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.name()).thenReturn(name);
    when(config.requiresReindex()).thenReturn(requiresReindex);
    // Default to an existing index — the incremental path is the common case. Fresh-create
    // tests override this to false.
    when(config.exists()).thenReturn(true);
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
