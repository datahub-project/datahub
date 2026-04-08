package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
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
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncrementalReindexCatchUpStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";

  @Mock private EntityService<?> entityService;
  @Mock private AspectDao aspectDao;
  @Mock private UpgradeContext upgradeContext;
  @Mock private Upgrade upgrade;

  private OperationContext opContext;
  private IncrementalReindexCatchUpStep step;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());

    step =
        new IncrementalReindexCatchUpStep(
            opContext, entityService, aspectDao, List.of(), Set.of(), UPGRADE_VERSION, false);
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
            false, // requiresDataBackfill = false
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    when(aspectDao.streamAspectBatches(any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Should still stream aspects for the T0 gap window
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
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

    verify(aspectDao, never()).streamAspectBatches(any());
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

    verify(aspectDao, never()).streamAspectBatches(any());
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
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);

    setupPhase1Result(phase1State);

    // Mock empty stream so we don't need real aspects
    when(aspectDao.streamAspectBatches(any()))
        .thenReturn(
            PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify the stream query was scoped to the dataset entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
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
            true,
            IncrementalReindexState.Status.COMPLETED);
    phase1State = IncrementalReindexState.setDualWriteStartTime(phase1State, index2, 2000L);

    setupPhase1Result(phase1State);

    // Return a fresh empty stream for each call (PartitionedStream is closed after use)
    when(aspectDao.streamAspectBatches(any()))
        .thenAnswer(
            invocation ->
                PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Verify two separate streams were opened, one per entity type
    ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao, org.mockito.Mockito.times(2)).streamAspectBatches(argsCaptor.capture());

    java.util.List<RestoreIndicesArgs> allArgs = argsCaptor.getAllValues();
    java.util.Set<String> urnLikes =
        allArgs.stream().map(args -> args.urnLike).collect(java.util.stream.Collectors.toSet());
    assertTrue(urnLikes.contains("urn:li:dataset:%"));
    assertTrue(urnLikes.contains("urn:li:chart:%"));
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
