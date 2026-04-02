package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IncrementalReindexAliasSwapStepTest {

  private static final String UPGRADE_VERSION = "0.14.0-0";
  private static final String INDEX_NAME = "datasetindex_v2";
  private static final String NEXT_INDEX_NAME = "datasetindex_v2_0_14_0-0_100";

  @Mock private EntityService<?> entityService;
  @Mock private ESIndexBuilder indexBuilder;
  @Mock private UpgradeContext upgradeContext;
  @Mock private Upgrade upgrade;

  private OperationContext opContext;
  private IncrementalReindexAliasSwapStep step;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.openMocks(this);
    opContext = TestOperationContexts.systemContextNoValidate();

    when(upgradeContext.opContext()).thenReturn(opContext);
    when(upgradeContext.upgrade()).thenReturn(upgrade);
    when(upgrade.getUpgradeResult(any(), any(), any())).thenReturn(Optional.empty());
    lenient()
        .when(entityService.getLatestEnvelopedAspect(any(), any(), any(), any()))
        .thenReturn(null);
    lenient()
        .when(entityService.ingestProposal(any(), any(), any(), anyBoolean()))
        .thenReturn(mock(IngestResult.class));

    step = createStep();
  }

  @Test
  public void testIdIncludesVersion() {
    assertEquals(step.id(), "IncrementalReindexAliasSwap_0.14.0-0");
  }

  @Test
  public void testSucceedsWhenNoPhase1State() {
    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipsIndicesNotCompleted() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.IN_PROGRESS);

    setupPhase1Result(phase1State);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipsAlreadySwappedIndices() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.ALIAS_SWAPPED);

    setupPhase1Result(phase1State);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testFailsWhenDocCountMismatch() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);

    setupPhase1Result(phase1State);
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(false);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testSwapsWhenDocCountsMatch() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);

    setupPhase1Result(phase1State);
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    verify(indexBuilder).validateAndSwapAlias(eq(INDEX_NAME), eq(NEXT_INDEX_NAME));
    // Phase 1 conditional persist + per-index IN_PROGRESS checkpoint + final SUCCEEDED checkpoint
    verify(entityService, times(3)).ingestProposal(eq(opContext), any(), any(), anyBoolean());
  }

  @Test
  public void testSwapsWhenBothIndicesEmpty() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);

    setupPhase1Result(phase1State);
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testSkipsWhenNextIndexNameMissing() throws Exception {
    Map<String, String> phase1State = new HashMap<>();
    phase1State.put(
        IncrementalReindexState.key(INDEX_NAME, IncrementalReindexState.STATUS),
        IncrementalReindexState.Status.COMPLETED.name());

    setupPhase1Result(phase1State);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testFailsWhenSwapThrowsException() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);

    setupPhase1Result(phase1State);
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME))
        .thenThrow(new RuntimeException("ES connection lost"));

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @Test
  public void testUpdatesPhase1StateOnSuccessfulSwap() throws Exception {
    Map<String, String> phase1State =
        IncrementalReindexState.setPhase1State(
            null,
            INDEX_NAME,
            NEXT_INDEX_NAME,
            100L,
            true,
            IncrementalReindexState.Status.COMPLETED);

    setupPhase1Result(phase1State);
    when(indexBuilder.validateAndSwapAlias(INDEX_NAME, NEXT_INDEX_NAME)).thenReturn(true);

    UpgradeStepResult result = step.executable().apply(upgradeContext);
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    // Phase 1 persist + alias-swap step checkpoint(s)
    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, times(3))
        .ingestProposal(eq(opContext), mcpCaptor.capture(), any(), anyBoolean());

    List<String> phase1Urns =
        mcpCaptor.getAllValues().stream()
            .map(m -> m.getEntityUrn().toString())
            .filter(u -> u.contains("BuildIndicesIncremental"))
            .collect(Collectors.toList());
    assertTrue(
        !phase1Urns.isEmpty(),
        "Expected at least one ingest to Phase 1 (BuildIndicesIncremental) URN");
  }

  private IncrementalReindexAliasSwapStep createStep() throws Exception {
    ElasticSearchIndexed mockService = mock(ElasticSearchIndexed.class);
    ReindexConfig mockConfig = mock(ReindexConfig.class);
    when(mockConfig.name()).thenReturn(INDEX_NAME);
    when(mockService.buildReindexConfigs(any(), any())).thenReturn(List.of(mockConfig));
    when(mockService.getIndexBuilder()).thenReturn(indexBuilder);

    return new IncrementalReindexAliasSwapStep(
        opContext, entityService, List.of(mockService), Set.of(), UPGRADE_VERSION);
  }

  private void setupPhase1Result(Map<String, String> phase1State) throws Exception {
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

    when(entityService.getLatestEnvelopedAspect(
            eq(opContext),
            eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME),
            org.mockito.ArgumentMatchers.argThat(
                urn -> urn != null && urn.toString().contains("BuildIndicesIncremental")),
            eq(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME)))
        .thenAnswer(
            invocation -> {
              EnvelopedAspect ea = new EnvelopedAspect();
              DataHubUpgradeResult d = new DataHubUpgradeResult();
              d.setState(DataHubUpgradeState.SUCCEEDED);
              d.setResult(new StringMap(phase1State));
              ea.setValue(new Aspect(d.data()));
              ea.setSystemMetadata(new SystemMetadata().setVersion("1"));
              return ea;
            });
  }
}
