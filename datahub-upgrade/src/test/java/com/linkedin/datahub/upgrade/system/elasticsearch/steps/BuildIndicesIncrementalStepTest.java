package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.Sha256UrnEntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder.IncrementalReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder.PollReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexMigrationStamp;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
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
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BuildIndicesIncrementalStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String NEXT_INDEX_NAME = "datasetindex_v2_0_14_0-0_1679000000000";
  private static final String V3_INDEX_NAME = "primaryindex_v3";
  private static final String OTHER_INDEX_NAME = "chartindex_v3";
  private static final Urn SAMPLED_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,sampled,PROD)");
  private static final String HASHED_ID =
      new Sha256UrnEntityDocumentIdHasher().documentId(OperationFingerprint.EMPTY, SAMPLED_URN);
  private static final String LEGACY_ID =
      "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2Csampled%2CPROD%29";

  @Mock private EntityService<?> entityService;
  @Mock private ESIndexBuilder indexBuilder;
  @Mock private ElasticSearchIndexed indexedService;
  @Mock private UpgradeContext upgradeContext;
  @Mock private Upgrade upgrade;
  @Mock private UpdateIndicesService updateIndicesService;

  private final V3DocumentIdScheme docIdScheme =
      new V3DocumentIdScheme(new Sha256UrnEntityDocumentIdHasher());

  private OperationContext opContext;
  private BuildIndicesIncrementalStep step;
  private BuildIndicesConfiguration buildIndicesConfig;

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
    when(indexedService.getIndexBuilder(anyString())).thenReturn(indexBuilder);
    when(indexBuilder.getBackingIndices(any(OperationContext.class), anyString()))
        .thenReturn(Set.of("datasetindex_v2_old"));
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), anyString(), anyString(), anyLong()))
        .thenReturn(true);
    when(indexBuilder.indexExists(any(OperationContext.class), anyString())).thenReturn(true);

    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(V3_INDEX_NAME));

    buildIndicesConfig =
        BuildIndicesConfiguration.builder().reconcileInPlaceMappingUpdates(false).build();
    step = createStep();
  }

  private BuildIndicesIncrementalStep createStep() {
    return new BuildIndicesIncrementalStep(
        opContext,
        List.of(indexedService),
        Set.of(),
        entityService,
        UPGRADE_VERSION,
        buildIndicesConfig,
        updateIndicesService,
        docIdScheme,
        true);
  }

  /** A filled-generation index whose live mapping records the running id scheme. */
  private ReindexConfig currentSchemeV3Config(boolean requiresReindex) {
    ReindexConfig config = mockReindexConfig(V3_INDEX_NAME, requiresReindex);
    when(config.currentMappings())
        .thenReturn(docIdScheme.stampedMappings(Map.of("properties", Map.of())));
    return config;
  }

  private Map<String, String> persistedState() {
    ArgumentCaptor<MetadataChangeProposal> proposals =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(), proposals.capture(), any(), anyBoolean());
    MetadataChangeProposal last = proposals.getValue();
    DataHubUpgradeResult result =
        GenericRecordUtils.deserializeAspect(
            last.getAspect().getValue(),
            last.getAspect().getContentType(),
            DataHubUpgradeResult.class);
    return result.getResult();
  }

  private static Urn phase1Urn() {
    return BootstrapStep.getUpgradeUrn(
        IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + UPGRADE_VERSION);
  }

  private static Urn catchUpUrn() {
    return BootstrapStep.getUpgradeUrn(
        IncrementalReindexState.CATCH_UP_UPGRADE_ID_PREFIX + "_" + UPGRADE_VERSION);
  }

  /**
   * Phase 1 row for its URN, and a catch-up row only when {@code catchUpState} is non-null. One
   * answer, because a later {@code any()} stub would hide an {@code eq()} stub.
   */
  private void stubUpgradeResults(
      Map<String, String> phase1State, @Nullable Map<String, String> catchUpState) {
    DataHubUpgradeResult phase1Result = mock(DataHubUpgradeResult.class);
    when(phase1Result.getResult()).thenReturn(new StringMap(phase1State));
    DataHubUpgradeResult catchUpResult = mock(DataHubUpgradeResult.class);
    if (catchUpState != null) {
      when(catchUpResult.getResult()).thenReturn(new StringMap(catchUpState));
    }
    String phase1 = phase1Urn().toString();
    String catchUp = catchUpUrn().toString();
    // The mock otherwise swallows Upgrade's default method, which is the upsert under test.
    doCallRealMethod()
        .when(upgrade)
        .setUpgradeResult(any(), any(), any(), nullable(DataHubUpgradeState.class), any());
    when(upgrade.getUpgradeResult(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Object urn = invocation.getArgument(1);
              if (urn != null && phase1.equals(urn.toString())) {
                return Optional.of(phase1Result);
              }
              if (catchUpState != null && urn != null && catchUp.equals(urn.toString())) {
                return Optional.of(catchUpResult);
              }
              return Optional.empty();
            });
  }

  private List<DataHubUpgradeResult> resultsFor(Urn urn) {
    ArgumentCaptor<MetadataChangeProposal> proposals =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(), proposals.capture(), any(), anyBoolean());
    return proposals.getAllValues().stream()
        .filter(proposal -> urn.toString().equals(String.valueOf(proposal.getEntityUrn())))
        .map(
            proposal ->
                GenericRecordUtils.deserializeAspect(
                    proposal.getAspect().getValue(),
                    proposal.getAspect().getContentType(),
                    DataHubUpgradeResult.class))
        .toList();
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
  public void testAppliesInPlaceMappingUpdateWithoutReconciliation() throws Throwable {
    ReindexConfig inPlaceConfig = mockReindexConfig(INDEX_NAME, false);
    when(inPlaceConfig.requiresMappingReconciliation()).thenReturn(true);
    when(inPlaceConfig.requiresApplyMappings()).thenReturn(true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(inPlaceConfig));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(inPlaceConfig));
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }

  @Test
  public void testPassesAnIndexWithUnmappedStructuredPropertiesToBuildIndex() throws Throwable {
    // Nothing is applied to it, but buildIndex reports the definitions it has no field for.
    ReindexConfig unchangedConfig = mockReindexConfig(INDEX_NAME, false);
    when(unchangedConfig.hasNewStructuredProperty()).thenReturn(true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(unchangedConfig));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(unchangedConfig));
  }

  @Test
  public void testReconcilesInPlaceMappingUpdateWhenEnabled() throws Throwable {
    buildIndicesConfig.setReconcileInPlaceMappingUpdates(true);
    step = createStep();

    ReindexConfig inPlaceConfig = mockReindexConfig(INDEX_NAME, false);
    when(inPlaceConfig.requiresMappingReconciliation()).thenReturn(true);
    when(inPlaceConfig.requiresApplyMappings()).thenReturn(true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(inPlaceConfig));

    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), eq(inPlaceConfig), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            eq("task1")))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 100L)));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .buildIndexIncremental(any(OperationContext.class), eq(inPlaceConfig), eq(UPGRADE_VERSION));
    verify(indexBuilder, never()).buildIndex(any(OperationContext.class), eq(inPlaceConfig));
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
        .validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong());
  }

  @Test
  public void testSwapGatesOnSourceCountFromReindexLaunch() throws Throwable {
    // The alias-swap gate must receive the source count snapshotted when the reindex was launched.
    // Gating on a live source count is unsatisfiable on an index that keeps taking writes for the
    // duration of the copy, which leaves the swap permanently unable to succeed.
    long sourceDocCountAtLaunch = 4200L;
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, sourceDocCountAtLaunch, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            eq("task1")))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 100L)));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .validateAndSwapAlias(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            eq(sourceDocCountAtLaunch));
  }

  @Test
  public void testResumedSwapGatesOnPersistedSourceCount() throws Throwable {
    // A resumed run has no fresh measurement to work from, so it must gate on the snapshot the
    // original reindex targeted rather than re-measuring the source.
    long persistedSourceDocCount = 7350L;
    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            null,
            1679000000000L,
            persistedSourceDocCount,
            null,
            false,
            IncrementalReindexState.Status.IN_PROGRESS);
    DataHubUpgradeResult upgradeResult = mock(DataHubUpgradeResult.class);
    when(upgradeResult.getResult()).thenReturn(new StringMap(previousState));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(upgradeResult));

    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            eq("")))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 100L)));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder)
        .validateAndSwapAlias(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            eq(persistedSourceDocCount));
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
        .validateAndSwapAlias(any(OperationContext.class), anyString(), anyString(), anyLong());
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
  public void testReindexConfigsAreCollectedOnlyOnceForMultipleIndices() throws Throwable {
    ReindexConfig first = mockReindexConfig("datasetindex_v2", false);
    ReindexConfig second = mockReindexConfig("chartindex_v2", false);
    when(first.exists()).thenReturn(false);
    when(second.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(first, second));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexedService, times(1)).buildReindexConfigs(any(), any());
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(first));
    verify(indexBuilder).buildIndex(any(OperationContext.class), eq(second));
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
        .validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong());
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
            any(OperationContext.class), eq(INDEX_NAME), eq(freshNextIndex), anyLong()))
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
        .validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(freshNextIndex), anyLong());
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
  public void testCompletedRowRecreatesAMissingIndex() throws Throwable {
    // Upgrade history can outlive the cluster it describes. The OpenSearch 3 volume cutover keeps
    // primary storage, so a COMPLETED row must not skip an index the new cluster does not have.
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));

    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX_NAME,
            V3_INDEX_NAME + "_old",
            null,
            1679000000000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);
    previousState.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        "primaryindex_v3_previous");
    previousState.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.DUAL_WRITE_START_TIME),
        "1000");
    previousState.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.TASK_ID), "old-task");
    previousState.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.REINDEX_COMPLETE_TIME),
        "2000");
    Map<String, String> catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            null, V3_INDEX_NAME, IncrementalReindexState.CatchUpStatus.COMPLETED);
    catchUpState.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.CATCH_UP_LAST_URN),
        "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)");
    catchUpState =
        IncrementalReindexState.setCatchUpStatus(
            catchUpState, OTHER_INDEX_NAME, IncrementalReindexState.CatchUpStatus.COMPLETED);
    stubUpgradeResults(previousState, catchUpState);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .createAliasedIndex(
            any(OperationContext.class), eq(V3_INDEX_NAME), anyString(), eq(v3Config), any());
    Map<String, String> recreated = persistedState();
    assertEquals(
        IncrementalReindexState.get(
            recreated, V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.empty(),
        "a recreated index has no old generation, so the previous backing name is not a rollback target");
    assertEquals(
        IncrementalReindexState.get(
            recreated, V3_INDEX_NAME, IncrementalReindexState.DUAL_WRITE_START_TIME),
        Optional.empty(),
        "the previous attempt's window end would make catch-up skip the new fill");
    assertEquals(
        IncrementalReindexState.get(recreated, V3_INDEX_NAME, IncrementalReindexState.TASK_ID),
        Optional.empty());
    assertEquals(
        IncrementalReindexState.get(
            recreated, V3_INDEX_NAME, IncrementalReindexState.REINDEX_COMPLETE_TIME),
        Optional.empty());

    List<DataHubUpgradeResult> cleared = resultsFor(catchUpUrn());
    assertEquals(cleared.size(), 1, "the replanned index's catch-up verdict is cleared once");
    assertEquals(cleared.get(0).getState(), DataHubUpgradeState.IN_PROGRESS);
    Map<String, String> clearedState = cleared.get(0).getResult();
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(clearedState, V3_INDEX_NAME), Optional.empty());
    assertEquals(
        IncrementalReindexState.get(
            clearedState, V3_INDEX_NAME, IncrementalReindexState.CATCH_UP_LAST_URN),
        Optional.empty());
    assertEquals(
        IncrementalReindexState.getCatchUpStatus(clearedState, OTHER_INDEX_NAME),
        Optional.of(IncrementalReindexState.CatchUpStatus.COMPLETED),
        "another index's catch-up verdict stays");
  }

  @Test
  public void testRecreatedIndexRowIsNotCompletedWhileTheIndexIsCreated() throws Throwable {
    // A crash after the index is created but before its fill row is checkpointed must not leave
    // the COMPLETED row behind: the next run would skip the index and never fill it.
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX_NAME,
            V3_INDEX_NAME + "_old",
            null,
            1679000000000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);
    stubUpgradeResults(previousState, null);
    AtomicReference<Optional<IncrementalReindexState.Status>> statusWhenCreated =
        new AtomicReference<>();
    doAnswer(
            invocation -> {
              // persistedState() fails when nothing was checkpointed before the index was created,
              // which is the state this test guards against.
              statusWhenCreated.set(
                  IncrementalReindexState.getStatus(persistedState(), V3_INDEX_NAME));
              throw new RuntimeException("simulated crash after index creation");
            })
        .when(indexBuilder)
        .createAliasedIndex(
            any(OperationContext.class), eq(V3_INDEX_NAME), anyString(), eq(v3Config), any());

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    assertEquals(
        statusWhenCreated.get(),
        Optional.of(IncrementalReindexState.Status.FAILED),
        "the row must not read COMPLETED while the index it guards is being created");
  }

  @Test
  public void testRecreatedIndexWithoutCatchUpResultWritesNone() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));

    Map<String, String> previousState =
        IncrementalReindexState.setPhase1State(
            null,
            V3_INDEX_NAME,
            V3_INDEX_NAME + "_old",
            null,
            1679000000000L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);
    stubUpgradeResults(previousState, null);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertTrue(resultsFor(catchUpUrn()).isEmpty(), "no catch-up result is created");
  }

  @Test
  public void testFailsWhenNoIndexBuilderFound() throws Throwable {
    // Return a reindex config for an index that no service provides a builder for
    ReindexConfig unknownConfig = mockReindexConfig("unknown_index", true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(unknownConfig));
    when(indexedService.getIndexBuilder(anyString())).thenReturn(null);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testCachedBuilderAvoidsRepeatingConfigDiscovery() throws Throwable {
    // Config discovery succeeds once. A second call would throw, proving the execution loop uses
    // the builder cache populated by getAllReindexConfigs instead of rebuilding every service's
    // configs for every index.
    ElasticSearchIndexed throwingService = mock(ElasticSearchIndexed.class);
    when(throwingService.buildReindexConfigs(any(), any()))
        .thenReturn(List.of())
        .thenThrow(new UnsupportedOperationException());

    step =
        new BuildIndicesIncrementalStep(
            opContext,
            List.of(throwingService, indexedService),
            Set.of(),
            entityService,
            UPGRADE_VERSION,
            buildIndicesConfig,
            updateIndicesService,
            docIdScheme,
            true);

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
    verify(throwingService, times(1)).buildReindexConfigs(any(), any());
    verify(indexedService, times(1)).buildReindexConfigs(any(), any());
    verify(indexBuilder)
        .buildIndexIncremental(any(OperationContext.class), any(), eq(UPGRADE_VERSION));
  }

  @Test
  public void testExceptionReturnsFailed() throws Throwable {
    when(indexBuilder.buildIndexIncremental(any(OperationContext.class), any(), anyString()))
        .thenThrow(new RuntimeException("ES connection error"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSwapFailurePersistsFailedAndCleansUpNextIndex() throws Throwable {
    // A reindex that completes but whose alias swap fails must be persisted as FAILED — not
    // COMPLETED (a rerun would hit "already COMPLETED, skipping" and silently succeed while the
    // alias still points at the stale index) and not IN_PROGRESS (a rerun would resume into a poll
    // it already satisfies and re-attempt the identical swap forever, never copying anything). Only
    // FAILED routes the next run to the fresh-start branch. The next index is deleted so that
    // rebuild starts clean.
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
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
        .thenReturn(false);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    // The run must fail loudly, not silently succeed.
    assertEquals(result.result(), DataHubUpgradeState.FAILED);

    Map<String, String> persisted = captureLastPersistedState();
    assertEquals(
        IncrementalReindexState.getStatus(persisted, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.FAILED));
    verify(indexBuilder).deleteActionWithRetry(any(OperationContext.class), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSwapThrowingAlsoPersistsFailed() throws Throwable {
    // An exception out of the swap must escalate exactly like a doc-count mismatch. Letting it
    // propagate to the step's outer catch would return FAILED while leaving the persisted status
    // IN_PROGRESS — the stranded state whose only possible retry action is the same failing swap.
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString()))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 90L)));
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
        .thenThrow(new RuntimeException("alias update rejected"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    Map<String, String> persisted = captureLastPersistedState();
    assertEquals(
        IncrementalReindexState.getStatus(persisted, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.FAILED));
    verify(indexBuilder).deleteActionWithRetry(any(OperationContext.class), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSwapThrowingButAliasAlreadySwapped_treatsAsSuccess() throws Throwable {
    // If rename/alias update succeeded and a subsequent exception was observed, cleanup must not
    // delete the live next index — verify backing and treat as success.
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString()))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 100L)));
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
        .thenThrow(new RuntimeException("alias update acknowledged but client timed out"));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(INDEX_NAME)))
        .thenReturn(Set.of(NEXT_INDEX_NAME));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    Map<String, String> persisted = captureLastPersistedState();
    assertEquals(
        IncrementalReindexState.getStatus(persisted, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
    verify(indexBuilder, never())
        .deleteActionWithRetry(any(OperationContext.class), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testSwapThrowingAndAliasVerifyFails_marksFailedWithoutCleanup() throws Throwable {
    // If getBackingIndices itself fails after a swap exception, we cannot tell whether the alias
    // already points at nextIndexName. Deleting would risk removing the live backing index — mark
    // FAILED without cleanup instead.
    IncrementalReindexResult incrementalResult =
        new IncrementalReindexResult(
            NEXT_INDEX_NAME, 1679000000000L, "task1", false, 2, 0L, Map.of());
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), any(), eq(UPGRADE_VERSION)))
        .thenReturn(incrementalResult);
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(INDEX_NAME),
            eq(NEXT_INDEX_NAME),
            any(),
            anyInt(),
            anyMap(),
            anyString()))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(100L, 100L)));
    when(indexBuilder.validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
        .thenThrow(new RuntimeException("alias update acknowledged but client timed out"));
    // First call resolves the old backing index before reindex; second is the post-swap verify.
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(INDEX_NAME)))
        .thenReturn(Set.of("datasetindex_v2_old"))
        .thenThrow(new RuntimeException("getAliases timed out"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    Map<String, String> persisted = captureLastPersistedState();
    assertEquals(
        IncrementalReindexState.getStatus(persisted, INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.FAILED));
    verify(indexBuilder, never())
        .deleteActionWithRetry(any(OperationContext.class), eq(NEXT_INDEX_NAME));
  }

  @Test
  public void testRerunAfterSwapFailureReindexesInsteadOfNoOpRetry() throws Throwable {
    // End-to-end: run 1 reindexes then fails the swap; run 2 (fed run 1's persisted state) must
    // REINDEX from scratch. Resuming instead would re-poll a target already satisfied by the
    // frozen destination, returning "complete" without copying anything, and then re-attempt the
    // identical swap — a retry that can never make progress and never transitions state.
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
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
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
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong()))
        .thenReturn(true);
    UpgradeStepResult run2 = step.executable().apply(upgradeContext);

    assertEquals(run2.result(), DataHubUpgradeState.SUCCEEDED);
    // Run 2 must have rebuilt the index (FAILED is neither skipped nor resumed), then swapped.
    verify(indexBuilder, times(2))
        .buildIndexIncremental(any(OperationContext.class), any(), eq(UPGRADE_VERSION));
    verify(indexBuilder, times(2))
        .validateAndSwapAlias(
            any(OperationContext.class), eq(INDEX_NAME), eq(NEXT_INDEX_NAME), anyLong());
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
    when(config.requiresMappingReconciliation()).thenReturn(false);
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

  @Test
  public void testFillIsRefusedBeforeAnyWorkWhenNothingCouldRunIt() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    step =
        new BuildIndicesIncrementalStep(
            opContext,
            List.of(indexedService),
            Set.of(),
            entityService,
            UPGRADE_VERSION,
            buildIndicesConfig,
            updateIndicesService,
            docIdScheme,
            false);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(entityService, never()).ingestProposal(any(), any(), any(), anyBoolean());
  }

  @Test
  public void testMissingFilledIndexIsCreatedBehindItsAliasAsAFillTarget() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.exists()).thenReturn(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<String> physical = ArgumentCaptor.forClass(String.class);
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<String, Object>> mappings = ArgumentCaptor.forClass(Map.class);
    verify(indexBuilder)
        .createAliasedIndex(
            any(OperationContext.class),
            eq(V3_INDEX_NAME),
            physical.capture(),
            eq(v3Config),
            mappings.capture());
    assertTrue(physical.getValue().startsWith(V3_INDEX_NAME + "_"));
    assertEquals(
        IndexMigrationStamp.read(mappings.getValue()).orElse(null), IndexMigrationStamp.INCOMPLETE);
    verify(indexBuilder, never())
        .markMigrationIncomplete(any(OperationContext.class), eq(physical.getValue()));
    verify(indexBuilder, never()).buildIndex(any(OperationContext.class), any());
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());

    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        Optional.of(V3_INDEX_NAME));
    assertEquals(
        IncrementalReindexState.get(state, V3_INDEX_NAME, IncrementalReindexState.NEXT_INDEX_NAME),
        Optional.of(physical.getValue()));
    assertEquals(
        IncrementalReindexState.getIndexVersion(state, V3_INDEX_NAME), EntityIndexVersion.V3);
  }

  @Test
  public void testFilledIndexFromAnotherIdSchemeIsRebuiltIntoANextIndex() throws Throwable {
    // Unstamped, and the sampled ids are not the running hasher's: a pre-hashing index.
    ReindexConfig v3Config = unstampedV3Config();
    stubSampledIds(Map.of(LEGACY_ID, SAMPLED_URN.toString()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<String> next = ArgumentCaptor.forClass(String.class);
    verify(indexBuilder).createIndex(any(OperationContext.class), next.capture(), eq(v3Config));
    assertTrue(next.getValue().startsWith(V3_INDEX_NAME + "_"));
    // A rebuild replaces the _reindex the mapping change would otherwise have triggered.
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder, never())
        .validateAndSwapAlias(any(OperationContext.class), anyString(), anyString(), anyLong());

    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        Optional.of(next.getValue()));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of("primaryindex_v3_old"));
    assertTrue(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME),
        "its sampled ids are not the running hasher's: no live write may be mirrored into it");
  }

  @Test
  public void testUnstampedIndexWhoseSampledIdsAreCurrentKeepsRollbackWrites() throws Throwable {
    // Built under the running hasher before stamping existed: rebuilt once to record the scheme,
    // and the index the rebuild leaves behind keeps receiving rollback writes.
    ReindexConfig v3Config = unstampedV3Config();
    stubSampledIds(Map.of(HASHED_ID, SAMPLED_URN.toString()));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<String> next = ArgumentCaptor.forClass(String.class);
    verify(indexBuilder).createIndex(any(OperationContext.class), next.capture(), eq(v3Config));
    assertTrue(next.getValue().startsWith(V3_INDEX_NAME + "_"));
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of("primaryindex_v3_old"));
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME),
        "its sampled ids are the running hasher's, so rollback writes address its documents");
  }

  @Test
  public void testUnstampedIndexWhoseSampleCannotBeReadGetsNoRollbackWrites() throws Throwable {
    ReindexConfig v3Config = unstampedV3Config();
    when(indexBuilder.sampleDocumentFieldById(
            any(OperationContext.class), eq(V3_INDEX_NAME), eq("urn"), anyInt()))
        .thenThrow(new IOException("search timed out"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).createIndex(any(OperationContext.class), anyString(), eq(v3Config));
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertTrue(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME),
        "nothing shows that writes into it would address its documents");
  }

  @Test
  public void testIndexStampedWithAnotherSchemeIsNotSampled() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, true);
    when(v3Config.currentMappings())
        .thenReturn(
            Map.of("_meta", Map.of("docIdScheme", "sha256-tenant-urn"), "properties", Map.of()));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(V3_INDEX_NAME)))
        .thenReturn(Set.of("primaryindex_v3_old"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).createIndex(any(OperationContext.class), anyString(), eq(v3Config));
    verify(indexBuilder, never())
        .sampleDocumentFieldById(any(OperationContext.class), anyString(), anyString(), anyInt());
    assertTrue(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(persistedState(), V3_INDEX_NAME),
        "a recorded scheme other than the running one answers by itself");
  }

  /**
   * A filled-generation index whose live mapping records no id scheme, behind one backing index.
   */
  private ReindexConfig unstampedV3Config() throws IOException {
    ReindexConfig config = mockReindexConfig(V3_INDEX_NAME, true);
    when(config.currentMappings()).thenReturn(Map.of("properties", Map.of()));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(config));
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(V3_INDEX_NAME)))
        .thenReturn(Set.of("primaryindex_v3_old"));
    return config;
  }

  private void stubSampledIds(Map<String, String> urnsById) throws IOException {
    when(indexBuilder.sampleDocumentFieldById(
            any(OperationContext.class), eq(V3_INDEX_NAME), eq("urn"), anyInt()))
        .thenReturn(urnsById);
  }

  @Test
  public void testEmptyCurrentSchemeFilledIndexIsFilledInPlace() throws Throwable {
    // Current scheme, no documents, nothing to apply: the one case filled where it stands.
    ReindexConfig v3Config = currentSchemeV3Config(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(0L);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());
    // Structured property fields the index lacks go in before the fill writes their values.
    InOrder fillOrder = inOrder(indexBuilder);
    fillOrder
        .verify(indexBuilder)
        .addUnmappedStructuredPropertyFields(any(OperationContext.class), eq(v3Config));
    fillOrder
        .verify(indexBuilder)
        .markMigrationIncomplete(any(OperationContext.class), eq(V3_INDEX_NAME));
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        Optional.of(V3_INDEX_NAME));
  }

  @Test
  public void testRejectedStructuredPropertyFieldsFailAFillInPlace() throws Throwable {
    // Filled without those fields, the index would hold their values unsearchable.
    ReindexConfig v3Config = currentSchemeV3Config(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(0L);
    doThrow(new IOException("Limit of total fields [1000] has been exceeded"))
        .when(indexBuilder)
        .addUnmappedStructuredPropertyFields(any(OperationContext.class), eq(v3Config));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(indexBuilder, never()).markMigrationIncomplete(any(OperationContext.class), anyString());
  }

  @Test
  public void testACompletedEmptyMigrationIsNotMarkedIncompleteAgain() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.currentMappings())
        .thenReturn(
            IndexMigrationStamp.stamped(
                docIdScheme.stampedMappings(Map.of("properties", Map.of())),
                IndexMigrationStamp.COMPLETE));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(0L);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());
    verify(indexBuilder, never()).markMigrationIncomplete(any(OperationContext.class), anyString());
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    assertTrue(IncrementalReindexState.getStatus(persistedState(), V3_INDEX_NAME).isEmpty());
  }

  @Test
  public void testUnmarkedPopulatedV3WithV2OnlyOnTheClusterIsRebuilt() throws Throwable {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    String v3 = convention.getEntityIndexNameV3(opContext, "dataset");
    String v2 = convention.getEntityIndexName(opContext, "dataset");
    ReindexConfig v3Config = mockReindexConfig(v3, false);
    when(v3Config.currentMappings())
        .thenReturn(docIdScheme.stampedMappings(Map.of("properties", Map.of())));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(v3));
    when(indexBuilder.getCount(any(OperationContext.class), eq(v3))).thenReturn(4L);
    when(indexBuilder.indexExistsOnV2Cluster(any(OperationContext.class), eq(v2))).thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).createIndex(any(OperationContext.class), anyString(), eq(v3Config));
  }

  @Test
  public void testUnmarkedPopulatedV3WithRetiredV2OnTheClusterIsLeftAlone() throws Throwable {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    String v3 = convention.getEntityIndexNameV3(opContext, "dataset");
    String v2 = convention.getEntityIndexName(opContext, "dataset");
    ReindexConfig v3Config = mockReindexConfig(v3, false);
    when(v3Config.currentMappings())
        .thenReturn(docIdScheme.stampedMappings(Map.of("properties", Map.of())));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(v3));
    when(indexBuilder.getCount(any(OperationContext.class), eq(v3))).thenReturn(4L);
    when(indexBuilder.indexExistsOnV2Cluster(any(OperationContext.class), eq(v2))).thenReturn(true);
    when(indexBuilder.v2CutoverRetired(any(OperationContext.class), eq(v2))).thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
  }

  @Test
  public void testUnmarkedPopulatedV3WithRetiredV2InTheBuildListIsLeftAlone() throws Throwable {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    String v3 = convention.getEntityIndexNameV3(opContext, "dataset");
    String v2 = convention.getEntityIndexName(opContext, "dataset");
    ReindexConfig v3Config = mockReindexConfig(v3, false);
    when(v3Config.currentMappings())
        .thenReturn(docIdScheme.stampedMappings(Map.of("properties", Map.of())));
    ReindexConfig v2Config = mockReindexConfig(v2, false);
    when(v2Config.currentMappings()).thenReturn(Map.of("_meta", Map.of("v3Cutover", "retired")));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config, v2Config));
    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(v3));
    when(indexBuilder.getCount(any(OperationContext.class), eq(v3))).thenReturn(4L);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
  }

  @Test
  public void testAnAbandonedMigrationIsRebuiltAndIsNotARollbackTarget() throws Throwable {
    // A later deploy has no Phase 1 row for the fill that wrote these documents. The index matches
    // the target, so only the incomplete stamp says the corpus is partial.
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.currentMappings())
        .thenReturn(
            IndexMigrationStamp.stamped(
                docIdScheme.stampedMappings(Map.of("properties", Map.of())),
                IndexMigrationStamp.INCOMPLETE));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(42L);
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(V3_INDEX_NAME)))
        .thenReturn(Set.of("primaryindex_v3_old"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<String> next = ArgumentCaptor.forClass(String.class);
    verify(indexBuilder).createIndex(any(OperationContext.class), next.capture(), eq(v3Config));
    assertTrue(next.getValue().startsWith(V3_INDEX_NAME + "_"));
    verify(indexBuilder).markMigrationIncomplete(any(OperationContext.class), eq(next.getValue()));
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        Optional.of(next.getValue()));
    assertTrue(IncrementalReindexState.isOldBackingIndexIncompleteMigration(state, V3_INDEX_NAME));
    assertFalse(IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME));
  }

  @Test
  public void testUnmarkedPopulatedIndexWithAPairedV2IndexIsRebuilt() throws Throwable {
    PairedIndices paired = firstPairedIndices();
    ReindexConfig v3Config = currentSchemeConfig(paired.v3Name());
    ReindexConfig v2Config = mockReindexConfig(paired.v2Name(), false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config, v2Config));
    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(paired.v3Name()));
    when(indexBuilder.getCount(any(OperationContext.class), eq(paired.v3Name()))).thenReturn(42L);
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(paired.v3Name())))
        .thenReturn(Set.of(paired.v3Name() + "_old"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).createIndex(any(OperationContext.class), anyString(), eq(v3Config));
    verify(indexBuilder).markMigrationIncomplete(any(OperationContext.class), anyString());
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }

  @Test
  public void testUnmarkedPopulatedIndexOwnedByThisDeployIsNotRebuiltAgain() throws Throwable {
    PairedIndices paired = firstPairedIndices();
    ReindexConfig v3Config = currentSchemeConfig(paired.v3Name());
    ReindexConfig v2Config = mockReindexConfig(paired.v2Name(), false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config, v2Config));
    when(updateIndicesService.indicesFilledFromPrimaryStorage(any(OperationContext.class)))
        .thenReturn(Set.of(paired.v3Name()));
    when(indexBuilder.getCount(any(OperationContext.class), eq(paired.v3Name()))).thenReturn(42L);
    Map<String, String> previous =
        IncrementalReindexState.setPhase1State(
            null,
            paired.v3Name(),
            paired.v3Name() + "_old",
            null,
            1L,
            0L,
            null,
            false,
            IncrementalReindexState.Status.COMPLETED);
    DataHubUpgradeResult previousResult = new DataHubUpgradeResult();
    previousResult.setState(DataHubUpgradeState.SUCCEEDED);
    previousResult.setResult(new StringMap(previous));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(indexBuilder, never()).markMigrationIncomplete(any(OperationContext.class), anyString());
  }

  private ReindexConfig currentSchemeConfig(String name) {
    ReindexConfig config = mockReindexConfig(name, false);
    when(config.currentMappings())
        .thenReturn(docIdScheme.stampedMappings(Map.of("properties", Map.of())));
    return config;
  }

  /** A V3 index and one V2 entity index whose documents live in it. */
  private PairedIndices firstPairedIndices() {
    IndexConvention convention = opContext.getSearchContext().getIndexConvention();
    for (EntitySpec spec : opContext.getEntityRegistry().getEntitySpecs().values()) {
      String v3Name = convention.getEntityIndexNameV3(opContext, V3IndexKeys.resolve(spec));
      String v2Name = convention.getEntityIndexName(opContext, spec.getName());
      if (!v3Name.isEmpty() && !v2Name.isEmpty()) {
        return new PairedIndices(v3Name, v2Name);
      }
    }
    throw new IllegalStateException("the test registry has no entity index");
  }

  private record PairedIndices(String v3Name, String v2Name) {}

  @Test
  public void testPopulatedCurrentSchemeFilledIndexNeedsNothing() throws Throwable {
    ReindexConfig v3Config = currentSchemeV3Config(false);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(42L);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }

  @Test
  public void testEmptyIndexWithMappingChangeIsRebuiltIntoANextIndex() throws Throwable {
    // Empty is the steady state of an entity type nobody has ingested. Filled where it stands, the
    // index would receive documents under a mapping Phase 1 still has to change, and the in-place
    // apply pass never reaches an index with a fill row.
    ReindexConfig v3Config = currentSchemeV3Config(true);
    when(v3Config.requiresApplyMappings()).thenReturn(true);

    assertEmptyIndexIsRebuiltIntoANextIndex(v3Config);
  }

  @Test
  public void testEmptyIndexWithAdditiveMappingChangeIsRebuiltIntoANextIndex() throws Throwable {
    ReindexConfig v3Config = currentSchemeV3Config(false);
    when(v3Config.requiresApplyMappings()).thenReturn(true);
    when(v3Config.isPureMappingsAddition()).thenReturn(true);

    assertEmptyIndexIsRebuiltIntoANextIndex(v3Config);
  }

  @Test
  public void testEmptyIndexWithSettingsChangeIsRebuiltIntoANextIndex() throws Throwable {
    ReindexConfig v3Config = currentSchemeV3Config(false);
    when(v3Config.requiresApplySettings()).thenReturn(true);

    assertEmptyIndexIsRebuiltIntoANextIndex(v3Config);
  }

  private void assertEmptyIndexIsRebuiltIntoANextIndex(ReindexConfig v3Config) throws Throwable {
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(0L);
    when(indexBuilder.getBackingIndices(any(OperationContext.class), eq(V3_INDEX_NAME)))
        .thenReturn(Set.of("primaryindex_v3_old"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    ArgumentCaptor<String> next = ArgumentCaptor.forClass(String.class);
    verify(indexBuilder).createIndex(any(OperationContext.class), next.capture(), eq(v3Config));
    assertTrue(next.getValue().startsWith(V3_INDEX_NAME + "_"));
    // Neither the empty-source _reindex swap, which would complete the row with no fill, nor an
    // in-place apply touches the live index.
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
    verify(indexBuilder, never()).buildIndex(any(OperationContext.class), eq(v3Config));
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());

    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX),
        Optional.of(next.getValue()));
    assertEquals(
        IncrementalReindexState.get(
            state, V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_NAME),
        Optional.of("primaryindex_v3_old"));
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME),
        "the old index's ids are current, so rollback writes may reach it");
    verify(indexBuilder, never())
        .sampleDocumentFieldById(any(OperationContext.class), anyString(), anyString(), anyInt());
  }

  @Test
  public void testFilledIndexWithMappingChangeTakesTheReindexPath() throws Throwable {
    // A populated, current-scheme V3 index with a mapping change is copied and swapped like a V2
    // index; its write gap is replayed by catch-up per entity type. No fill row is planned.
    String v3Next = V3_INDEX_NAME + "_0_14_0-0_1679000000000";
    ReindexConfig v3Config = currentSchemeV3Config(true);
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    when(indexBuilder.getCount(any(OperationContext.class), eq(V3_INDEX_NAME))).thenReturn(42L);
    when(indexBuilder.buildIndexIncremental(
            any(OperationContext.class), eq(v3Config), eq(UPGRADE_VERSION)))
        .thenReturn(
            new IncrementalReindexResult(v3Next, 1679000000000L, "task3", false, 2, 42L, Map.of()));
    when(indexBuilder.pollReindexCompletion(
            any(OperationContext.class),
            eq(V3_INDEX_NAME),
            eq(v3Next),
            any(),
            anyInt(),
            anyMap(),
            eq("task3")))
        .thenReturn(new PollReindexResult(true, Map.of(), Pair.of(42L, 42L)));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder)
        .validateAndSwapAlias(any(OperationContext.class), eq(V3_INDEX_NAME), eq(v3Next), eq(42L));
    verify(indexBuilder, never())
        .createAliasedIndex(any(OperationContext.class), anyString(), anyString(), any(), any());
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.COMPLETED));
    assertEquals(
        IncrementalReindexState.getIndexVersion(state, V3_INDEX_NAME), EntityIndexVersion.V3);
    assertFalse(
        state.containsKey(
            IncrementalReindexState.key(
                V3_INDEX_NAME, IncrementalReindexState.BACKFILL_TARGET_INDEX)));
  }

  @Test
  public void testAReplannedFillStartsWithoutThePreviousAttemptsProgress() throws Throwable {
    // The previous attempt filled its types, failed at the end and was marked FAILED with its
    // cursor and verdicts still on the row; the new fill must not resume at that cursor.
    ReindexConfig v3Config = unstampedV3Config();
    stubSampledIds(Map.of(HASHED_ID, SAMPLED_URN.toString()));
    Map<String, String> previous = new java.util.HashMap<>();
    previous.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.FAILED.name());
    previous.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.BACKFILL_CURSOR_ENTITY),
        "dataset");
    previous.put(
        IncrementalReindexState.key(V3_INDEX_NAME, IncrementalReindexState.BACKFILL_URNS_PROCESSED),
        "7");
    previous.put(
        IncrementalReindexState.key(
            V3_INDEX_NAME, IncrementalReindexState.OLD_BACKING_INDEX_ID_SCHEME_STALE),
        "true");
    DataHubUpgradeResult previousResult = new DataHubUpgradeResult();
    previousResult.setState(DataHubUpgradeState.FAILED);
    previousResult.setResult(new StringMap(previous));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder).createIndex(any(OperationContext.class), anyString(), eq(v3Config));
    Map<String, String> state = persistedState();
    assertEquals(
        IncrementalReindexState.getStatus(state, V3_INDEX_NAME),
        Optional.of(IncrementalReindexState.Status.BACKFILL_PENDING));
    assertTrue(IncrementalReindexState.getBackfillCheckpoint(state, V3_INDEX_NAME).isEmpty());
    assertTrue(
        IncrementalReindexState.get(
                state, V3_INDEX_NAME, IncrementalReindexState.BACKFILL_URNS_PROCESSED)
            .isEmpty());
    assertFalse(
        IncrementalReindexState.isOldBackingIndexIdSchemeStale(state, V3_INDEX_NAME),
        "the earlier attempt's verdict does not carry over; the sample decides again");
  }

  @Test
  public void testFillInFlightIsLeftToTheNonBlockingPhaseOnRerun() throws Throwable {
    ReindexConfig v3Config = mockReindexConfig(V3_INDEX_NAME, false);
    when(v3Config.currentMappings()).thenReturn(Map.of("properties", Map.of()));
    when(indexedService.buildReindexConfigs(any(), any())).thenReturn(List.of(v3Config));
    Map<String, String> previous =
        IncrementalReindexState.setBackfillPending(
            null, V3_INDEX_NAME, V3_INDEX_NAME + "_0_14_0-0_1679000000000", 1000L);
    DataHubUpgradeResult previousResult = new DataHubUpgradeResult();
    previousResult.setState(DataHubUpgradeState.IN_PROGRESS);
    previousResult.setResult(new StringMap(previous));
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.of(previousResult));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    verify(indexBuilder, never()).createIndex(any(OperationContext.class), anyString(), any());
    verify(indexBuilder, never())
        .buildIndexIncremental(any(OperationContext.class), any(), anyString());
  }
}
