package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncrementalReindexCatchUpStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String TIMESERIES_INDEX = "dataset_datasetprofileaspect_v1";

  @Mock private EntityService<?> entityService;
  @Mock private AspectDao aspectDao;
  @Mock private UpgradeContext upgradeContext;
  @Mock private Upgrade upgrade;
  @Mock private ElasticSearchIndexed indexedService;
  @Mock private ESIndexBuilder indexBuilder;

  private OperationContext opContext;
  private IncrementalReindexCatchUpStep step;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    when(entityService.getLatestEnvelopedAspect(any(), any(), any(), any())).thenReturn(null);
    when(entityService.ingestProposal(any(), any(), any(), anyBoolean()))
        .thenReturn(mock(IngestResult.class));

    step =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            new BuildIndicesConfiguration());
  }

  @Test
  public void testIdIncludesVersion() {
    assertEquals(step.id(), "IncrementalReindexCatchUp_0.14.0-0");
  }

  @Test
  public void testSucceedsWhenNoPhase1State() {
    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testCatchesUpSettingsOnlyIndices() {
    // Even settings-only reindexes (requiresDataBackfill=false) need catch-up for the T0 gap
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            false, // requiresDataBackfill = false
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should still stream aspects for the T0 gap window
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.urnLike, "urn:li:dataset:%");
    assertEquals(capturedArgs.gePitEpochMs, 1000L);
    assertEquals(capturedArgs.lePitEpochMs, 2000L);
  }

  @Test
  public void testSkipsWhenCatchUpWindowEmpty() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            100L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    // Set dual write start time equal to reindex start time (empty window)
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 100L);

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              return Optional.empty();
            });

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any());
  }

  @Test
  public void testSkipsWhenMissingReindexStartTime() {
    // Manually build state without reindexStartTime
    Map<String, String> phase1State = new HashMap<>();
    phase1State.put(
        IncrementalReindexState.key(INDEX_NAME, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.COMPLETED.name());
    phase1State.put(
        IncrementalReindexState.key(INDEX_NAME, IncrementalReindexState.REQUIRES_DATA_BACKFILL),
        "true");
    // No REINDEX_START_TIME set

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              return Optional.empty();
            });

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any());
  }

  @Test
  public void testEntityScopingViaUrnLike() {
    // Set up Phase 1 state with a valid catch-up window
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    // Mock empty stream so we don't need real aspects
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify the stream query was scoped to the dataset entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.urnLike, "urn:li:dataset:%");
    assertEquals(capturedArgs.gePitEpochMs, 1000L);
    assertEquals(capturedArgs.lePitEpochMs, 2000L);
    assertTrue(capturedArgs.urnBasedPagination);
  }

  @Test
  public void testPerIndexResumeKeys() {
    // Set up two indices that both need catch-up
    String index2 = "chartindex_v2";
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);
    phase1State =
        IncrementalReindexState.setPhase1State(
            phase1State,
            index2,
            "chartindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, index2, 2000L);

    setupPhase1Result(phase1State);

    // Return a fresh empty stream for each call (PartitionedStream is closed after use)
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenAnswer(
            invocation ->
                PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify two separate streams were opened, one per entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, org.mockito.Mockito.times(2))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture());

    java.util.List<RestoreIndicesArgs> allArgs = argsCaptor.getAllValues();
    java.util.Set<String> urnLikes =
        allArgs.stream().map(args -> args.urnLike).collect(java.util.stream.Collectors.toSet());
    assertTrue(urnLikes.contains("urn:li:dataset:%"));
    assertTrue(urnLikes.contains("urn:li:chart:%"));
  }

  @Test
  public void testGlobalIndexCatchUpUsesUnscopedUrnLike() {
    // Graph index is a "global" index — catch-up should emit MCLs for ALL entities (urnLike = "%")
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String graphIndexName = indexConvention.getIndexName(ElasticSearchGraphService.INDEX_NAME);

    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            graphIndexName,
            graphIndexName + "_next",
            graphIndexName + "_old",
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, graphIndexName, 2000L);

    setupPhase1Result(phase1State);

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture());
    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.urnLike, "%");
  }

  @Test
  public void testMarksDualWriteDisabledWhenRollbackNotEnabled() {
    // When rollbackDualWriteEnabled=false, catch-up should mark completed indices as
    // DUAL_WRITE_DISABLED
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              return Optional.empty();
            });

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    // rollbackDualWriteEnabled defaults to false in BuildIndicesConfiguration
    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should have called ingestProposal to persist DUAL_WRITE_DISABLED on the Phase 1 URN
    verify(entityService, atLeastOnce()).ingestProposal(eq(opContext), any(), any(), eq(false));
  }

  @Test
  public void testSuccessPersistsCatchUpStateWithResultMap() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<DataHubUpgradeState> stateCaptor =
        ArgumentCaptor.forClass(DataHubUpgradeState.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> resultCaptor = ArgumentCaptor.forClass(Map.class);
    verify(upgrade, atLeastOnce())
        .setUpgradeResult(
            eq(opContext), any(), eq(entityService), stateCaptor.capture(), resultCaptor.capture());

    assertEquals(
        stateCaptor.getAllValues().get(stateCaptor.getAllValues().size() - 1),
        DataHubUpgradeState.SUCCEEDED);
    Map<String, String> finalResult =
        resultCaptor.getAllValues().get(resultCaptor.getAllValues().size() - 1);
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(finalResult, INDEX_NAME),
        Optional.of(IncrementalReindexState.CatchUpStatus.COMPLETED));
    assertTrue(
        resultCaptor.getAllValues().stream().noneMatch(map -> map == null),
        "Catch-up upgrade result map must not be cleared on success");
  }

  @Test
  public void testEarlyExitWhenCatchUpAlreadyComplete() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, INDEX_NAME, IncrementalReindexState.CatchUpStatus.COMPLETED);

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    DataHubUpgradeResult catchUpResult = mock(DataHubUpgradeResult.class);
    when(catchUpResult.getResult()).thenReturn(new StringMap(catchUpState));
    when(catchUpResult.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              if (urnArg.toString().contains("IncrementalReindexCatchUp")) {
                return Optional.of(catchUpResult);
              }
              return Optional.empty();
            });

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any());
  }

  @Test
  public void testSetCatchUpStatusRoundTrip() {
    Map<String, String> state =
        IncrementalReindexState.setCatchUpStatus(
            null, INDEX_NAME, IncrementalReindexState.CatchUpStatus.SKIPPED);
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(state, INDEX_NAME),
        Optional.of(IncrementalReindexState.CatchUpStatus.SKIPPED));
  }

  @Test
  public void testTimeseriesMissingOldBackingIndexSkipsCatchUp() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            TIMESERIES_INDEX + "_next",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    setupPhase1Result(phase1State);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).submitFilteredReindex(any(), any(), any(), any(), anyInt());
  }

  @Test
  public void testTimeseriesIndexNotFoundSkipsCatchUp() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            TIMESERIES_INDEX + "_next",
            TIMESERIES_INDEX + "_old",
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    setupPhase1Result(phase1State);

    ReindexConfig config = org.mockito.Mockito.mock(ReindexConfig.class);
    when(config.name()).thenReturn(TIMESERIES_INDEX);
    when(config.targetSettings()).thenReturn(Map.of("index", Map.of("number_of_shards", 1)));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(config));
    when(indexedService.getIndexBuilder()).thenReturn(indexBuilder);
    when(indexBuilder.submitFilteredReindex(any(), any(), any(), any(QueryBuilder.class), anyInt()))
        .thenThrow(new RuntimeException("index_not_found_exception: no such index"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testUnresolvedIndexSkipsCatchUp() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            "unknown_index_v1",
            "unknown_index_v1_next",
            "unknown_index_v1_old",
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, "unknown_index_v1", 2000L);

    setupPhase1Result(phase1State);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any());
  }

  @Test
  public void testTimeseriesCatchUpCompletesSuccessfully() throws Throwable {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            TIMESERIES_INDEX + "_next",
            TIMESERIES_INDEX + "_old",
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    setupPhase1Result(phase1State);

    ReindexConfig config = org.mockito.Mockito.mock(ReindexConfig.class);
    when(config.name()).thenReturn(TIMESERIES_INDEX);
    when(config.targetSettings()).thenReturn(Map.of("index", Map.of("number_of_shards", 1)));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(config));
    when(indexedService.getIndexBuilder()).thenReturn(indexBuilder);
    doReturn("task-1")
        .when(indexBuilder)
        .submitFilteredReindex(any(), any(), any(), any(QueryBuilder.class), anyInt());
    doReturn(Optional.empty()).when(indexBuilder).getTaskInfoByHeader(any(), any());
    when(indexBuilder.computeTimeoutAt()).thenReturn(System.currentTimeMillis() + 60_000L);

    BuildIndicesConfiguration configWithPoll = new BuildIndicesConfiguration();
    configWithPoll.setTaskPollIntervalSeconds(0);
    IncrementalReindexCatchUpStep timeseriesStep =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            configWithPoll);

    UpgradeStepResult result = timeseriesStep.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .submitFilteredReindex(
            eq(opContext),
            eq(TIMESERIES_INDEX + "_old"),
            eq(TIMESERIES_INDEX + "_next"),
            any(QueryBuilder.class),
            anyInt());
  }

  @Test
  public void testTimeseriesWithoutIndexBuilderSkipsCatchUp() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            TIMESERIES_INDEX,
            TIMESERIES_INDEX + "_next",
            TIMESERIES_INDEX + "_old",
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State =
        IncrementalReindexState.setDualWriteStartTime(phase1State, TIMESERIES_INDEX, 2000L);

    setupPhase1Result(phase1State);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).submitFilteredReindex(any(), any(), any(), any(), anyInt());
  }

  @Test
  public void testEntityCatchUpFailureReturnsFailed() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any()))
        .thenThrow(new RuntimeException("stream failed"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  private void setupPhase1Result(Map<String, String> phase1State) {
    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);

    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              return Optional.empty();
            });
  }
}
