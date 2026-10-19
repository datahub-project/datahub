package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.BackfillHeaders;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
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

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should still stream aspects for the T0 gap window
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.urnLike, "urn:li:dataset:%");
    assertEquals(capturedArgs.gePitEpochMs, 1000L);
    assertEquals(capturedArgs.lePitEpochMs, 2000L);
    assertEquals(capturedArgs.batchSize, 50);
  }

  @Test
  public void testCatchUpUsesConfiguredSqlPageSize() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);
    setupPhase1Result(phase1State);

    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setCatchUpSqlPageSize(25);
    IncrementalReindexCatchUpStep configuredStep =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            config);

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    configuredStep.executable().apply(upgradeContext);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    assertEquals(argsCaptor.getValue().batchSize, 25);
  }

  @Test
  public void testShouldFlushTriggersOnRowOrByteThreshold() {
    assertTrue(IncrementalReindexCatchUpStep.shouldFlush(500, 0, 500, 0));
    assertFalse(IncrementalReindexCatchUpStep.shouldFlush(499, 0, 500, 0));
    assertTrue(IncrementalReindexCatchUpStep.shouldFlush(1, 1024, 500, 1024));
    assertFalse(IncrementalReindexCatchUpStep.shouldFlush(1, 1023, 500, 1024));
    assertFalse(IncrementalReindexCatchUpStep.shouldFlush(499, 999999, 500, 0));
  }

  @Test
  public void testMetadataColumnCharLengthUsesRawStringLength() {
    assertEquals(IncrementalReindexCatchUpStep.metadataColumnCharLength("abc"), 3);
    assertEquals(IncrementalReindexCatchUpStep.metadataColumnCharLength(""), 0);
    assertEquals(IncrementalReindexCatchUpStep.metadataColumnCharLength(null), 0);
  }

  @Test
  public void testFlushEventProducerOnRowInterval() throws Exception {
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

    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setCatchUpSqlPageSize(50);
    config.setCatchUpFlushInterval(500);
    config.setCatchUpFlushBytesThreshold(0);
    IncrementalReindexCatchUpStep flushStep =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            config);

    SystemAspect mockAspect = createMockSystemAspect("urn:li:dataset:ds1");

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(streamWithPagedAspects(600, 50)));

    when(entityService.alwaysProduceMCLAsync(any(), any(), any(), any(MetadataChangeLog.class)))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    try (MockedStatic<EntityUtils> entityUtilsMock = mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<EbeanAspectV2> aspects = invocation.getArgument(2);
                return aspects.stream().map(a -> mockAspect).toList();
              });

      flushStep.executable().apply(upgradeContext);
    }

    verify(entityService, times(2)).flushEventProducer();
  }

  @Test
  public void testReplayedChangeLogsAreMarkedAsCatchUp() throws Exception {
    // A replayed row can describe an entity that was hard-deleted and recreated after the row was
    // read; only the catch-up mark makes the V3 writer rebuild from primary storage instead of
    // overlaying the pre-delete value, so every replayed change log must carry it.
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
    SystemAspect mockAspect = createMockSystemAspect("urn:li:dataset:ds1");
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(streamWithPagedAspects(3, 50)));
    when(entityService.alwaysProduceMCLAsync(any(), any(), any(), any(MetadataChangeLog.class)))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    try (MockedStatic<EntityUtils> entityUtilsMock = mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<EbeanAspectV2> aspects = invocation.getArgument(2);
                return aspects.stream().map(a -> mockAspect).toList();
              });
      step.executable().apply(upgradeContext);
    }

    ArgumentCaptor<MetadataChangeLog> changeLogs = ArgumentCaptor.forClass(MetadataChangeLog.class);
    verify(entityService, atLeastOnce())
        .alwaysProduceMCLAsync(any(), any(), any(), changeLogs.capture());
    assertEquals(changeLogs.getAllValues().size(), 3);
    for (MetadataChangeLog changeLog : changeLogs.getAllValues()) {
      assertEquals(changeLog.getChangeType(), ChangeType.RESTATE);
      assertEquals(changeLog.getEntityUrn(), mockAspect.getUrn());
      assertEquals(
          changeLog.getHeaders().get(BackfillHeaders.MODE_HEADER_NAME),
          BackfillHeaders.MODE_CATCH_UP);
    }
  }

  @Test
  public void testDefersFutureGetWhileUnderFlushLimit() throws Exception {
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

    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setCatchUpSqlPageSize(50);
    config.setCatchUpFlushInterval(500);
    config.setCatchUpFlushBytesThreshold(0);
    IncrementalReindexCatchUpStep flushStep =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            config);

    SystemAspect mockAspect = createMockSystemAspect("urn:li:dataset:ds1");
    AtomicInteger pending = new AtomicInteger();
    AtomicInteger maxPending = new AtomicInteger();

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(streamWithPagedAspects(600, 50)));

    when(entityService.alwaysProduceMCLAsync(any(), any(), any(), any(MetadataChangeLog.class)))
        .thenAnswer(
            invocation -> {
              pending.incrementAndGet();
              maxPending.updateAndGet(current -> Math.max(current, pending.get()));
              Future<?> future = mock(Future.class);
              when(future.get())
                  .thenAnswer(
                      i -> {
                        pending.decrementAndGet();
                        return null;
                      });
              return Pair.of(future, true);
            });

    try (MockedStatic<EntityUtils> entityUtilsMock = mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<EbeanAspectV2> aspects = invocation.getArgument(2);
                return aspects.stream().map(a -> mockAspect).toList();
              });

      flushStep.executable().apply(upgradeContext);
    }

    assertEquals(maxPending.get(), 500);
    assertEquals(pending.get(), 0);
  }

  @Test
  public void testFlushEventProducerOnByteThresholdBeforeRowInterval() throws Exception {
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

    BuildIndicesConfiguration config = new BuildIndicesConfiguration();
    config.setCatchUpSqlPageSize(2);
    config.setCatchUpFlushInterval(10_000);
    config.setCatchUpFlushBytesThreshold(1000L);
    IncrementalReindexCatchUpStep flushStep =
        new IncrementalReindexCatchUpStep(
            opContext,
            entityService,
            aspectDao,
            List.of(indexedService),
            Set.of(),
            UPGRADE_VERSION,
            config);

    SystemAspect mockAspect = createMockSystemAspect("urn:li:dataset:ds1");

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(streamWithLargePagedAspects(4, 2, 600)));

    when(entityService.alwaysProduceMCLAsync(any(), any(), any(), any(MetadataChangeLog.class)))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    try (MockedStatic<EntityUtils> entityUtilsMock = mockStatic(EntityUtils.class)) {
      entityUtilsMock
          .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                List<EbeanAspectV2> aspects = invocation.getArgument(2);
                return aspects.stream().map(a -> mockAspect).toList();
              });

      flushStep.executable().apply(upgradeContext);
    }

    verify(entityService, times(2)).flushEventProducer();
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

    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
  }

  @Test
  public void testFillInFlightGetsNoCatchUpStatusHere() {
    // The fill replays its own window once it completes; recording SKIPPED here would make that
    // later pass find the row already terminal.
    String v3Index = "primaryindex_v3";
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            v3Index,
            v3Index + "_0_14_0-0_1679000000000",
            null,
            1000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.BACKFILL_PENDING);
    phase1State = IncrementalReindexState.setBackfillPending(phase1State, v3Index, v3Index, 1000L);

    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
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
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
    ArgumentCaptor<MetadataChangeProposal> proposals =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    // Whatever this run persisted, none of it may carry a catch-up status for the fill row.
    verify(entityService, atLeast(0))
        .ingestProposal(any(), proposals.capture(), any(), anyBoolean());
    for (MetadataChangeProposal proposal : proposals.getAllValues()) {
      DataHubUpgradeResult persisted =
          GenericRecordUtils.deserializeAspect(
              proposal.getAspect().getValue(),
              proposal.getAspect().getContentType(),
              DataHubUpgradeResult.class);
      assertFalse(
          persisted.getResult() != null
              && persisted
                  .getResult()
                  .containsKey(
                      IncrementalReindexState.key(
                          v3Index, IncrementalReindexState.CATCH_UP_STATUS)),
          "fill rows must not receive a catch-up status from this step");
    }
    // And nothing this run persisted may let a later run treat catch-up as finished: the fill
    // row has a window and no terminal status, so the next run still has work to do here.
    assertFalse(IncrementalReindexState.isCatchUpCompleteForAllIndices(phase1State, Map.of()));
  }

  @Test
  public void testFailedFillDropsAStoredCatchUpStatusAndDoesNotReplay() {
    Map<String, String> phase1State = failedFill(INDEX_NAME);
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, INDEX_NAME, IncrementalReindexState.CatchUpStatus.COMPLETED);
    // The cursor belongs to the failed attempt's window. Keeping it would resume the rebuilt
    // index's catch-up partway through. Another index's cursor is not this fill's.
    catchUpState.put(INDEX_NAME + ".lastUrn", "urn:li:dataset:stale");
    catchUpState.put(INDEX_NAME + ".dataset.lastUrn", "urn:li:dataset:stale-type");
    catchUpState.put("chartindex_v2.lastUrn", "urn:li:chart:kept");
    stubUpgradeResults(phase1State, catchUpState, DataHubUpgradeState.SUCCEEDED);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
    Map<String, String> persisted = lastPersistedCatchUpResult();
    assertEquals(IncrementalReindexState.getCatchUpStatus(persisted, INDEX_NAME), Optional.empty());
    assertFalse(persisted.containsKey(INDEX_NAME + ".lastUrn"));
    assertFalse(persisted.containsKey(INDEX_NAME + ".dataset.lastUrn"));
    assertEquals(persisted.get("chartindex_v2.lastUrn"), "urn:li:chart:kept");
  }

  @Test
  public void testFailedFillDropsAResumeCursorLeftWithoutAVerdict() {
    Map<String, String> phase1State = failedFill(INDEX_NAME);
    Map<String, String> catchUpState = new HashMap<>();
    // A crashed replay checkpoints the cursor before it records a verdict.
    catchUpState.put(INDEX_NAME + ".lastUrn", "urn:li:dataset:stale");
    catchUpState.put(INDEX_NAME + ".dataset.lastUrn", "urn:li:dataset:stale-type");
    catchUpState.put("chartindex_v2.lastUrn", "urn:li:chart:kept");
    stubUpgradeResults(phase1State, catchUpState, DataHubUpgradeState.IN_PROGRESS);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
    Map<String, String> persisted = lastPersistedCatchUpResult();
    assertEquals(IncrementalReindexState.getCatchUpStatus(persisted, INDEX_NAME), Optional.empty());
    assertFalse(persisted.containsKey(INDEX_NAME + ".lastUrn"));
    assertFalse(persisted.containsKey(INDEX_NAME + ".dataset.lastUrn"));
    assertEquals(persisted.get("chartindex_v2.lastUrn"), "urn:li:chart:kept");
  }

  @Test
  public void testAFailedRowDoesNotSkipCatchUpOfACompletedPeer() {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            "chartindex_v2",
            "chartindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.FAILED);
    phase1State =
        IncrementalReindexState.setPhase1State(
            phase1State,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_100",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);
    stubUpgradeResults(phase1State, Map.of(), DataHubUpgradeState.IN_PROGRESS);
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    ArgumentCaptor<RestoreIndicesArgs> args = ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), args.capture(), any());
    assertEquals(args.getValue().urnLike, "urn:li:dataset:%");
  }

  @Test
  public void testCatchUpRunsOnceAFailedFillHasCompleted() {
    // First run: the fill is FAILED, and the earlier attempt left a verdict and a resume cursor.
    Map<String, String> staleCatchUp =
        IncrementalReindexState.setCatchUpStatus(
            null, INDEX_NAME, IncrementalReindexState.CatchUpStatus.COMPLETED);
    staleCatchUp.put(INDEX_NAME + ".lastUrn", "urn:li:dataset:stale");
    stubUpgradeResults(failedFill(INDEX_NAME), staleCatchUp, DataHubUpgradeState.SUCCEEDED);

    assertEquals(step.executable().apply(upgradeContext).result(), DataHubUpgradeState.FAILED);
    Map<String, String> afterFailure = lastPersistedCatchUpResult();

    // Second run: Phase 1 planned the index again and it completed. The replay covers the whole
    // window from the start; the dropped cursor is not resumed, and the verdict is recorded afresh.
    Map<String, String> completed =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            "datasetindex_v2_0_14_0-0_200",
            null,
            1000L,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    completed = IncrementalReindexState.setDualWriteStartTime(completed, INDEX_NAME, 2000L);
    stubUpgradeResults(completed, afterFailure, DataHubUpgradeState.IN_PROGRESS);
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    assertNull(argsCaptor.getValue().lastUrn, "the dropped cursor is not resumed");
    assertEquals(
        argsCaptor.getValue().gePitEpochMs, 1000L, "the window is replayed from its start");
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(lastPersistedCatchUpResult(), INDEX_NAME),
        Optional.of(IncrementalReindexState.CatchUpStatus.COMPLETED));
  }

  private static Map<String, String> failedFill(String indexName) {
    return IncrementalReindexState.setPhase1State(
        null,
        indexName,
        indexName + "_0_14_0-0_100",
        null,
        1000L,
        0L,
        null,
        true,
        IncrementalReindexState.Status.FAILED);
  }

  private void stubUpgradeResults(
      Map<String, String> phase1State,
      Map<String, String> catchUpState,
      DataHubUpgradeState catchUpUpgradeState) {
    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    DataHubUpgradeResult catchUpResult = mock(DataHubUpgradeResult.class);
    when(catchUpResult.getResult()).thenReturn(new StringMap(catchUpState));
    when(catchUpResult.getState()).thenReturn(catchUpUpgradeState);
    // doAnswer so a second call in the same test does not invoke the earlier answer with matchers.
    doAnswer(
            invocation -> {
              Object urnArg = invocation.getArgument(1);
              if (urnArg.toString().contains("BuildIndicesIncremental")) {
                return Optional.of(phase1Result);
              }
              if (urnArg.toString().contains("IncrementalReindexCatchUp")) {
                return Optional.of(catchUpResult);
              }
              return Optional.empty();
            })
        .when(upgrade)
        .getUpgradeResult(any(), any(), any());
  }

  private Map<String, String> lastPersistedCatchUpResult() {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, String>> resultCaptor = ArgumentCaptor.forClass(Map.class);
    verify(upgrade, atLeastOnce())
        .setUpgradeResult(eq(opContext), any(), eq(entityService), any(), resultCaptor.capture());
    return resultCaptor.getAllValues().get(resultCaptor.getAllValues().size() - 1);
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

    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
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
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify the stream query was scoped to the dataset entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
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
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            invocation ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        invocation.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify two separate streams were opened, one per entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, org.mockito.Mockito.times(2))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());

    java.util.List<RestoreIndicesArgs> allArgs = argsCaptor.getAllValues();
    java.util.Set<String> urnLikes =
        allArgs.stream().map(args -> args.urnLike).collect(java.util.stream.Collectors.toSet());
    assertTrue(urnLikes.contains("urn:li:dataset:%"));
    assertTrue(urnLikes.contains("urn:li:chart:%"));
  }

  @Test
  public void testV3IndexReplaysEveryEntityTypeFromTheFillStart() throws Exception {
    String alias = v3Alias();
    List<String> entityNames = v3EntityNames();
    // reindexStartTime differs from backfillStartTime here only to show which one opens the window.
    setupPhase1Result(completedV3Row(alias, 1000L, 5000L));
    stubIndexBuilderFor(alias);
    mockEmptyAspectStream();

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, times(entityNames.size()))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    List<RestoreIndicesArgs> allArgs = argsCaptor.getAllValues();
    assertEquals(
        allArgs.stream().map(args -> args.urnLike).collect(Collectors.toList()),
        entityNames.stream().map(name -> "urn:li:" + name + ":%").collect(Collectors.toList()));
    for (RestoreIndicesArgs args : allArgs) {
      assertEquals(args.gePitEpochMs, 1000L);
      // No old backing index means no dual-write start; "now" closes the window.
      assertTrue(args.lePitEpochMs > 5000L);
    }
  }

  @Test
  public void testV3ReindexRowUsesTheReindexWindow() throws Exception {
    String alias = v3Alias();
    Map<String, String> phase1State = completedV3Row(alias, null, 1000L);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, alias, 2000L);
    setupPhase1Result(phase1State);
    mockEmptyAspectStream();

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, times(v3EntityNames().size()))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    for (RestoreIndicesArgs args : argsCaptor.getAllValues()) {
      assertEquals(args.gePitEpochMs, 1000L);
      assertEquals(args.lePitEpochMs, 2000L);
    }
    // A _reindex row's index carries no migration mark.
    verify(indexBuilder, never()).markMigrationComplete(any(OperationContext.class), anyString());
  }

  @Test
  public void testV3ResumeCursorIsKeyedByEntityType() throws Exception {
    String alias = v3Alias();
    List<String> entityNames = v3EntityNames();
    String entityType = entityNames.get(0);
    String resumeUrn = "urn:li:" + entityType + ":resume";
    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(completedV3Row(alias, 1000L, 1000L)));
    stubIndexBuilderFor(alias);
    when(phase1Result.getState()).thenReturn(DataHubUpgradeState.SUCCEEDED);
    DataHubUpgradeResult catchUpResult = mock(DataHubUpgradeResult.class);
    when(catchUpResult.getResult())
        .thenReturn(
            new StringMap(
                Map.of(
                    alias + "." + entityType + "." + IncrementalReindexCatchUpStep.LAST_URN_KEY,
                    resumeUrn)));
    when(catchUpResult.getState()).thenReturn(DataHubUpgradeState.IN_PROGRESS);
    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation ->
                invocation.getArgument(1).toString().contains("BuildIndicesIncremental")
                    ? Optional.of(phase1Result)
                    : Optional.of(catchUpResult));
    mockEmptyAspectStream();

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, times(entityNames.size()))
        .streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
    Map<String, String> lastUrnByType =
        argsCaptor.getAllValues().stream()
            .collect(
                java.util.HashMap::new,
                (map, args) -> map.put(args.urnLike, args.lastUrn),
                Map::putAll);
    assertEquals(lastUrnByType.get("urn:li:" + entityType + ":%"), resumeUrn);
  }

  @Test
  public void testGlobalIndexCatchUpUsesUnscopedUrnLike() {
    // Graph index is a "global" index — catch-up should emit MCLs for ALL entities (urnLike = "%")
    IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
    String graphIndexName =
        indexConvention.getIndexName(
            opContext, SearchComponent.GRAPH, ElasticSearchGraphService.INDEX_NAME);

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

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(any(OperationContext.class), argsCaptor.capture(), any());
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

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

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

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            inv ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        inv.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));

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
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
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
    when(indexedService.getIndexBuilder(anyString())).thenReturn(indexBuilder);
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
    verify(aspectDao, never()).streamAspectBatches(any(OperationContext.class), any(), any());
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
    when(indexedService.getIndexBuilder(anyString())).thenReturn(indexBuilder);
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

    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenThrow(new RuntimeException("stream failed"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testACompletedCatchUpMarksTheIndexMigrated() throws Exception {
    String alias = v3Alias();
    setupPhase1Result(completedV3Row(alias, 1000L, 1000L));
    mockEmptyAspectStream();
    stubIndexBuilderFor(alias);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).markMigrationComplete(opContext, alias);
  }

  @Test
  public void testAFailedCatchUpLeavesTheIndexMarkedUnmigrated() throws Exception {
    String alias = v3Alias();
    setupPhase1Result(completedV3Row(alias, 1000L, 1000L));
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenThrow(new RuntimeException("stream failed"));
    stubIndexBuilderFor(alias);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never()).markMigrationComplete(any(OperationContext.class), anyString());
  }

  @Test
  public void testAMigrationMarkThatCannotBeWrittenFailsTheIndex() throws Exception {
    String alias = v3Alias();
    setupPhase1Result(completedV3Row(alias, 1000L, 1000L));
    mockEmptyAspectStream();
    stubIndexBuilderFor(alias);
    doThrow(new IOException("put mapping refused"))
        .when(indexBuilder)
        .markMigrationComplete(opContext, alias);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(lastPersistedCatchUpResult(), alias),
        Optional.of(IncrementalReindexState.CatchUpStatus.FAILED));
  }

  private void stubIndexBuilderFor(String indexName) throws Exception {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.name()).thenReturn(indexName);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(config));
    when(indexedService.getIndexBuilder(anyString())).thenReturn(indexBuilder);
  }

  private String v3Alias() {
    String key = V3IndexKeys.resolve(opContext.getEntityRegistry().getEntitySpec("dataset"));
    return opContext.getSearchContext().getIndexConvention().getEntityIndexNameV3(opContext, key);
  }

  private List<String> v3EntityNames() {
    String key = V3IndexKeys.resolve(opContext.getEntityRegistry().getEntitySpec("dataset"));
    return V3IndexKeys.entitySpecsForKey(opContext.getEntityRegistry(), key).stream()
        .map(EntitySpec::getName)
        .sorted()
        .collect(Collectors.toList());
  }

  /** A V3 row after Phase 1 and, when {@code backfillStartTime} is given, after its fill. */
  private static Map<String, String> completedV3Row(
      String alias, Long backfillStartTime, long reindexStartTime) {
    Map<String, String> state =
        IncrementalReindexState.setPhase1State(
            null,
            alias,
            alias + "_0_14_0-0_1679000000000",
            null,
            reindexStartTime,
            0L,
            null,
            true,
            IncrementalReindexState.Status.COMPLETED);
    if (backfillStartTime != null) {
      state = IncrementalReindexState.setBackfillPending(state, alias, alias, backfillStartTime);
      state = IncrementalReindexState.setPhase1Completed(state, alias);
    }
    return IncrementalReindexState.setIndexVersion(state, alias, EntityIndexVersion.V3);
  }

  private void mockEmptyAspectStream() {
    when(aspectDao.streamAspectBatches(any(OperationContext.class), any(), any()))
        .thenAnswer(
            invocation ->
                ((java.util.function.Function<PartitionedStream<EbeanAspectV2>, Object>)
                        invocation.getArgument(2))
                    .apply(
                        PartitionedStream.<EbeanAspectV2>builder()
                            .delegateStream(Stream.empty())
                            .build()));
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

  private SystemAspect createMockSystemAspect(String urnStr) {
    SystemAspect aspect = mock(SystemAspect.class);
    when(aspect.getUrn()).thenReturn(UrnUtils.getUrn(urnStr));
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn("testAspect");
    when(aspect.getAspectSpec()).thenReturn(aspectSpec);
    when(aspect.getRecordTemplate()).thenReturn(null);
    SystemMetadata systemMetadata = new SystemMetadata();
    when(aspect.getSystemMetadata()).thenReturn(systemMetadata);
    return aspect;
  }

  private PartitionedStream<EbeanAspectV2> streamWithPagedAspects(int totalAspects, int pageSize) {
    return aspectsStream(totalAspects, "{}");
  }

  private PartitionedStream<EbeanAspectV2> streamWithLargePagedAspects(
      int totalAspects, int pageSize, int metadataLength) {
    return aspectsStream(totalAspects, "x".repeat(metadataLength));
  }

  private PartitionedStream<EbeanAspectV2> aspectsStream(int totalAspects, String metadata) {
    List<EbeanAspectV2> aspects = new ArrayList<>();
    Timestamp now = new Timestamp(System.currentTimeMillis());
    for (int i = 0; i < totalAspects; i++) {
      aspects.add(
          new EbeanAspectV2(
              "urn:li:dataset:ds" + i,
              "datasetProperties",
              0L,
              metadata,
              now,
              "tester",
              null,
              null));
    }
    return PartitionedStream.<EbeanAspectV2>builder().delegateStream(aspects.stream()).build();
  }
}
