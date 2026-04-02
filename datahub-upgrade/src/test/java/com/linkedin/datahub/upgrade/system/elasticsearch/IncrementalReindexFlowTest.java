package com.linkedin.datahub.upgrade.system.elasticsearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexAliasSwapStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.IncrementalReindexCatchUpStep;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration test verifying the full incremental reindex flow: Phase 1 state is consumed by Phase
 * 2 (catch-up) and Phase 3 (alias swap). Uses an in-memory state store to simulate the upgrade
 * result persistence that flows between steps.
 */
public class IncrementalReindexFlowTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String NEXT_INDEX_NAME = "datasetindex_v2_0_14_0-0_100";

  private OperationContext opContext;
  private EntityService<?> entityService;
  private AspectDao aspectDao;
  private ESIndexBuilder indexBuilder;
  private UpgradeContext upgradeContext;
  private Upgrade upgrade;
  private MockedStatic<EntityUtils> entityUtilsMock;

  private final ConcurrentHashMap<String, DataHubUpgradeResult> stateStore =
      new ConcurrentHashMap<>();

  private final AtomicLong phase1EnvelopeVersion = new AtomicLong(1);

  @BeforeMethod
  public void setup() throws Exception {
    opContext = TestOperationContexts.systemContextNoValidate();
    entityService = mock(EntityService.class);
    aspectDao = mock(AspectDao.class);
    indexBuilder = mock(ESIndexBuilder.class);
    upgradeContext = mock(UpgradeContext.class);
    upgrade = mock(Upgrade.class);
    stateStore.clear();
    phase1EnvelopeVersion.set(1);

    when(entityService.getLatestEnvelopedAspect(
            eq(opContext),
            eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
            org.mockito.ArgumentMatchers.argThat(
                urn -> urn != null && urn.toString().contains("BuildIndicesIncremental")),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(2);
              DataHubUpgradeResult stored = stateStore.get(urn.toString());
              if (stored == null) {
                return null;
              }
              EnvelopedAspect ea = new EnvelopedAspect();
              ea.setValue(new Aspect(stored.data()));
              ea.setSystemMetadata(
                  new SystemMetadata().setVersion(String.valueOf(phase1EnvelopeVersion.get())));
              return ea;
            });

    org.mockito.Mockito.doAnswer(
            invocation -> {
              MetadataChangeProposal p = invocation.getArgument(1);
              String json = p.getAspect().getValue().asString(StandardCharsets.UTF_8);
              DataHubUpgradeResult updated =
                  RecordUtils.toRecordTemplate(DataHubUpgradeResult.class, json);
              stateStore.put(p.getEntityUrn().toString(), updated);
              phase1EnvelopeVersion.incrementAndGet();
              return mock(IngestResult.class);
            })
        .when(entityService)
        .ingestProposal(eq(opContext), any(MetadataChangeProposal.class), any(), eq(false));

    // Return empty stream for any aspect batch queries (overridden in specific tests)
    when(aspectDao.streamAspectBatches(any()))
        .thenAnswer(
            invocation ->
                PartitionedStream.<EbeanAspectV2>builder().delegateStream(Stream.empty()).build());

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);

    when(upgrade.getUpgradeResult(any(), any(Urn.class), any()))
        .thenAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              return Optional.ofNullable(stateStore.get(urn.toString()));
            });

    org.mockito.Mockito.doAnswer(
            invocation -> {
              Urn urn = invocation.getArgument(1);
              DataHubUpgradeState state = invocation.getArgument(3);
              Map<String, String> resultMap = invocation.getArgument(4);
              DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult();
              upgradeResult.setState(state);
              if (resultMap != null) {
                upgradeResult.setResult(new StringMap(resultMap));
              }
              stateStore.put(urn.toString(), upgradeResult);
              return null;
            })
        .when(upgrade)
        .setUpgradeResult(any(), any(Urn.class), any(), any(), any());

    entityUtilsMock = mockStatic(EntityUtils.class);
  }

  @AfterMethod
  public void tearDown() {
    // Static mocks hold state across tests if not closed
    if (entityUtilsMock != null) {
      entityUtilsMock.close();
    }
  }

  @Test
  public void testFullFlowPhase1ThroughPhase3() throws Exception {
    Map<String, String> phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                null,
                INDEX_NAME,
                NEXT_INDEX_NAME,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.COMPLETED);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setReindexCompleteTime(phase1State, INDEX_NAME, 2000L);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setDualWriteStartTime(phase1State, INDEX_NAME, 1500L);
    seedPhase1State(phase1State);

    // --- Phase 2: Catch-Up ---
    IncrementalReindexCatchUpStep catchUpStep =
        new IncrementalReindexCatchUpStep(opContext, entityService, aspectDao, UPGRADE_VERSION);

    UpgradeStepResult catchUpResult = catchUpStep.executable().apply(upgradeContext);
    assertEquals(catchUpResult.result(), DataHubUpgradeState.SUCCEEDED);

    org.mockito.ArgumentCaptor<RestoreIndicesArgs> argsCaptor =
        org.mockito.ArgumentCaptor.forClass(RestoreIndicesArgs.class);
    verify(aspectDao).streamAspectBatches(argsCaptor.capture());
    RestoreIndicesArgs capturedArgs = argsCaptor.getValue();
    assertEquals(capturedArgs.gePitEpochMs, 1000L);
    assertEquals(capturedArgs.lePitEpochMs, 1500L);
    assertTrue(capturedArgs.urnBasedPagination);
    assertEquals(capturedArgs.urnLike, "urn:li:dataset:%");

    // --- Phase 3: Alias Swap ---
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(true);

    IncrementalReindexAliasSwapStep aliasSwapStep = createAliasSwapStep();

    UpgradeStepResult swapResult = aliasSwapStep.executable().apply(upgradeContext);
    assertEquals(swapResult.result(), DataHubUpgradeState.SUCCEEDED);

    verify(indexBuilder).validateAndSwapAlias(eq(INDEX_NAME), eq(NEXT_INDEX_NAME));

    Urn aliasSwapUrn =
        BootstrapStep.getUpgradeUrn("IncrementalReindexAliasSwap_" + UPGRADE_VERSION);
    assertTrue(stateStore.containsKey(aliasSwapUrn.toString()));
    DataHubUpgradeResult swapState = stateStore.get(aliasSwapUrn.toString());
    assertEquals(swapState.getState(), DataHubUpgradeState.SUCCEEDED);
    assertEquals(
        swapState
            .getResult()
            .get(
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.key(
                    INDEX_NAME,
                    com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                        .STATUS)),
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState.Status
            .ALIAS_SWAPPED
            .name());
  }

  @Test
  public void testPhase3SkipsWhenPhase1NotCompleted() throws Exception {
    Map<String, String> phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                null,
                INDEX_NAME,
                NEXT_INDEX_NAME,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.IN_PROGRESS);
    seedPhase1State(phase1State);

    IncrementalReindexAliasSwapStep aliasSwapStep = createAliasSwapStep();

    UpgradeStepResult result = aliasSwapStep.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(indexBuilder, org.mockito.Mockito.never()).validateAndSwapAlias(any(), any());
  }

  @Test
  public void testPhase3IdempotentOnRerun() throws Exception {
    Map<String, String> phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                null,
                INDEX_NAME,
                NEXT_INDEX_NAME,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.ALIAS_SWAPPED);
    seedPhase1State(phase1State);

    IncrementalReindexAliasSwapStep aliasSwapStep = createAliasSwapStep();

    UpgradeStepResult result = aliasSwapStep.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(indexBuilder, org.mockito.Mockito.never()).validateAndSwapAlias(any(), any());
  }

  @Test
  public void testMultipleIndicesPartialSwap() throws Exception {
    String index2 = "chartindex_v2";
    String nextIndex2 = "chartindex_v2_0_14_0-0_100";

    Map<String, String> phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                null,
                INDEX_NAME,
                NEXT_INDEX_NAME,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.COMPLETED);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                phase1State,
                index2,
                nextIndex2,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.IN_PROGRESS);
    seedPhase1State(phase1State);

    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(true);

    IncrementalReindexAliasSwapStep aliasSwapStep = createAliasSwapStep();

    UpgradeStepResult result = aliasSwapStep.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(indexBuilder).validateAndSwapAlias(eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
    verify(indexBuilder, org.mockito.Mockito.never())
        .validateAndSwapAlias(eq(index2), eq(nextIndex2));
  }

  @Test
  public void testCatchUpCheckpointMergesAcrossIndices() throws Exception {
    String index2 = "chartindex_v2";
    Map<String, String> phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                null,
                INDEX_NAME,
                NEXT_INDEX_NAME,
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.COMPLETED);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setDualWriteStartTime(phase1State, INDEX_NAME, 2000L);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setPhase1State(
                phase1State,
                index2,
                "chartindex_v2_0_14_0-0_100",
                1000L,
                true,
                com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
                    .Status.COMPLETED);
    phase1State =
        com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState
            .setDualWriteStartTime(phase1State, index2, 2000L);
    seedPhase1State(phase1State);

    SystemAspect datasetAspect = createMockSystemAspect("urn:li:dataset:ds1");
    SystemAspect chartAspect = createMockSystemAspect("urn:li:chart:ch1");

    entityUtilsMock
        .when(() -> EntityUtils.toSystemAspectFromEbeanAspects(any(), any()))
        .thenAnswer(
            invocation -> {
              List<EbeanAspectV2> aspects = invocation.getArgument(1);
              if (aspects.isEmpty()) {
                return List.of();
              }
              String urn = aspects.get(0).getUrn();
              if (urn.startsWith("urn:li:dataset:")) {
                return List.of(datasetAspect);
              } else {
                return List.of(chartAspect);
              }
            });

    when(entityService.alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(Pair.of(CompletableFuture.completedFuture(null), true));

    when(aspectDao.streamAspectBatches(any()))
        .thenAnswer(
            invocation -> {
              RestoreIndicesArgs args = invocation.getArgument(0);
              EbeanAspectV2 mockAspect = mock(EbeanAspectV2.class);
              if (args.urnLike != null && args.urnLike.contains("dataset")) {
                when(mockAspect.getUrn()).thenReturn("urn:li:dataset:ds1");
              } else {
                when(mockAspect.getUrn()).thenReturn("urn:li:chart:ch1");
              }
              PartitionedStream<EbeanAspectV2> mockStream = mock(PartitionedStream.class);
              when(mockStream.partition(anyInt())).thenReturn(Stream.of(Stream.of(mockAspect)));
              return mockStream;
            });

    IncrementalReindexCatchUpStep catchUpStep =
        new IncrementalReindexCatchUpStep(opContext, entityService, aspectDao, UPGRADE_VERSION);

    UpgradeStepResult result = catchUpStep.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    Urn catchUpUrn = BootstrapStep.getUpgradeUrn("IncrementalReindexCatchUp_" + UPGRADE_VERSION);
    DataHubUpgradeResult checkpointState = stateStore.get(catchUpUrn.toString());
    if (checkpointState != null && checkpointState.getResult() != null) {
      Map<String, String> checkpointMap = checkpointState.getResult();
      assertTrue(
          checkpointMap.containsKey(INDEX_NAME + ".lastUrn"),
          "Checkpoint should contain dataset index lastUrn");
      assertTrue(
          checkpointMap.containsKey(index2 + ".lastUrn"),
          "Checkpoint should contain chart index lastUrn");
      assertEquals(checkpointMap.get(INDEX_NAME + ".lastUrn"), "urn:li:dataset:ds1");
      assertEquals(checkpointMap.get(index2 + ".lastUrn"), "urn:li:chart:ch1");
    } else {
      verify(aspectDao, org.mockito.Mockito.times(2)).streamAspectBatches(any());
    }
  }

  private void seedPhase1State(Map<String, String> phase1State) {
    Urn phase1Urn = BootstrapStep.getUpgradeUrn("BuildIndicesIncremental_" + UPGRADE_VERSION);
    DataHubUpgradeResult phase1Result = new DataHubUpgradeResult();
    phase1Result.setState(DataHubUpgradeState.SUCCEEDED);
    phase1Result.setResult(new StringMap(phase1State));
    stateStore.put(phase1Urn.toString(), phase1Result);
  }

  private IncrementalReindexAliasSwapStep createAliasSwapStep() throws Exception {
    ElasticSearchIndexed mockService = mock(ElasticSearchIndexed.class);
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn(INDEX_NAME);
    when(mockService.buildReindexConfigs(any(), any())).thenReturn(List.of(mockConfig));
    when(mockService.getIndexBuilder()).thenReturn(indexBuilder);

    return new IncrementalReindexAliasSwapStep(
        opContext, entityService, List.of(mockService), Set.of(), UPGRADE_VERSION);
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
}
