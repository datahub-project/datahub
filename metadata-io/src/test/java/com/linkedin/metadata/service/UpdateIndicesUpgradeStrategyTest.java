package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateIndicesUpgradeStrategyTest {

  @Mock private ElasticSearchService elasticSearchService;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private MCLItem mockEvent;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;
  @Mock private RecordTemplate mockAspect;
  @Mock private AuditStamp mockAuditStamp;
  @Mock private MetadataChangeLog mockMcl;

  private OperationContext operationContext;
  private Urn testUrn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    when(mockEvent.getUrn()).thenReturn(testUrn);
    when(mockEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockEvent.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockEvent.getRecordTemplate()).thenReturn(mockAspect);
    when(mockEvent.getAuditStamp()).thenReturn(mockAuditStamp);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEvent.getMetadataChangeLog()).thenReturn(mockMcl);
    when(mockMcl.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    AspectSpec keyAspectSpec = mock(AspectSpec.class);
    when(keyAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(keyAspectSpec);
  }

  @Test
  public void testIsEnabledWithNoTargets() {
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService,
            searchDocumentTransformer,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            0);
    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testIsEnabledWithTargets() {
    Map<String, String> targets = Map.of("dataset", "datasetindex_v2_next_123");
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);
    assertTrue(strategy.isEnabled());
  }

  @Test
  public void testProcessBatchNoOpWhenNoTargets() {
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService,
            searchDocumentTransformer,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            0);

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatchWritesToNextIndex() throws Exception {
    String nextIndex = "datasetindex_v2_next_123";
    Map<String, String> targets = Map.of("dataset", nextIndex);
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", testUrn.toString());
    searchDoc.put("name", "SampleHdfsDataset");

    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService)
        .upsertDocumentByIndexName(
            eq(operationContext), eq(nextIndex), eq(searchDoc.toString()), anyString());
  }

  /**
   * The two sides of {@code oldIndexTargets} disagree on case: the map is keyed from {@link
   * com.linkedin.metadata.utils.elasticsearch.IndexConvention#getEntityName}, which strips the
   * lowercased physical index ("dataflowindex_v2" -> "dataflow"), while lookups use {@code
   * EntitySpec.getName()}, the entity-registry name ("dataFlow"). Before normalisation the lookup
   * missed and dual-write silently never ran for any entity whose registered name is not
   * all-lowercase — dataFlow, dataJob, corpUser, mlModel, glossaryTerm, aiAgent.
   */
  @Test
  public void testProcessBatchWritesToNextIndexForMixedCaseEntity() throws Exception {
    String nextIndex = "dataflowindex_v2_next_123";
    Map<String, String> targets = Map.of("dataflow", nextIndex);
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    Urn dataFlowUrn = UrnUtils.getUrn("urn:li:dataFlow:(airflow,my_dag,PROD)");
    when(mockEvent.getUrn()).thenReturn(dataFlowUrn);
    when(mockEntitySpec.getName()).thenReturn("dataFlow");

    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", dataFlowUrn.toString());

    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(dataFlowUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService)
        .upsertDocumentByIndexName(
            eq(operationContext), eq(nextIndex), eq(searchDoc.toString()), anyString());
  }

  /** Callers hold the registry name; the map was keyed from the lowercased index name. */
  @Test
  public void testRemoveTargetIsCaseInsensitive() {
    Map<String, String> targets = new HashMap<>(Map.of("dataflow", "dataflowindex_v2_next_123"));
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    assertTrue(strategy.isEnabled());

    strategy.removeTarget("dataFlow");

    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testProcessBatchSkipsUnmatchedEntity() throws Exception {
    // Target is for "chart" entity, but event is for "dataset"
    Map<String, String> targets = Map.of("chart", "chartindex_v2_next_123");
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
    verify(searchDocumentTransformer, never())
        .transformAspect(any(), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void testProcessBatchSkipsEmptySearchDocument() throws Exception {
    Map<String, String> targets = Map.of("dataset", "datasetindex_v2_next_123");
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.empty());

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatchDeleteKeyAspect() throws Exception {
    String nextIndex = "datasetindex_v2_next_123";
    Map<String, String> targets = Map.of("dataset", nextIndex);
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    // Configure as key aspect deletion
    when(mockMcl.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey");

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService)
        .deleteDocumentByIndexName(eq(operationContext), eq(nextIndex), anyString());
  }

  @Test
  public void testDualWriteStartTimeCallbackCalledOnce() throws Exception {
    String nextIndex = "datasetindex_v2_next_123";
    Map<String, String> targets = Map.of("dataset", nextIndex);

    AtomicLong capturedTime = new AtomicLong(0);
    AtomicReference<String> capturedIndex = new AtomicReference<>();
    UpdateIndicesUpgradeStrategy.DualWriteStartTimeCallback callback =
        (indexName, startTime) -> {
          capturedIndex.set(indexName);
          capturedTime.set(startTime);
        };

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService,
            searchDocumentTransformer,
            targets,
            callback,
            null,
            null,
            null,
            0);

    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", testUrn.toString());

    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    // First call should trigger callback
    strategy.processBatch(operationContext, events, false);
    assertTrue(capturedTime.get() > 0);
    assertEquals(capturedIndex.get(), "dataset");

    // Second call should NOT trigger callback again
    capturedTime.set(0);
    strategy.processBatch(operationContext, events, false);
    assertEquals(capturedTime.get(), 0L); // not updated
  }

  /**
   * A callback that throws must not be mistaken for a successful persist. The start time is
   * recorded once per index, so treating a transient failure as done loses it permanently and
   * leaves Phase 2's catch-up step without a query window.
   */
  @Test
  public void testDualWriteStartTimeRetriedAfterCallbackFailure() throws Exception {
    Map<String, String> targets = Map.of("dataset", "datasetindex_v2_next_123");

    AtomicLong attempts = new AtomicLong(0);
    UpdateIndicesUpgradeStrategy.DualWriteStartTimeCallback callback =
        (indexName, startTime) -> {
          if (attempts.incrementAndGet() == 1) {
            throw new IllegalStateException("GMS unreachable");
          }
        };

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService,
            searchDocumentTransformer,
            targets,
            callback,
            null,
            null,
            null,
            0);

    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", testUrn.toString());
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    strategy.processBatch(operationContext, events, false);
    strategy.processBatch(operationContext, events, false);

    assertEquals(attempts.get(), 2L, "failed persist should be retried on the next batch");
  }

  /**
   * Reconcile has to add, not only remove. The initial load is a network call in restli
   * deployments, and a GMS that is not serving yet at consumer startup previously left the target
   * map empty for the whole process lifetime — dual-write silently off, old backing index going
   * stale.
   */
  @Test
  public void testReconcileAddsTargetAfterAFailedInitialLoad() throws Exception {
    Urn upgradeIdUrn = UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService,
            searchDocumentTransformer,
            Collections.emptyMap(),
            null,
            null,
            null,
            null,
            0);

    assertFalse(strategy.isEnabled(), "starts with nothing, as if the initial read had failed");

    strategy.reconcileTargets(
        operationContext,
        storeReturning(
            upgradeResultAspect(
                "datasetindex_v2",
                "datasetindex_v2_next_123",
                "datasetindex_v2_old_456",
                IncrementalReindexState.Status.COMPLETED)),
        upgradeIdUrn);

    assertTrue(strategy.isEnabled(), "a later successful read must recover the target");
  }

  /** A read failure is not the same as "no state": dropping targets would stop the protection. */
  @Test
  public void testReconcileKeepsTargetsWhenTheReadFails() {
    Urn upgradeIdUrn = UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");
    Map<String, String> targets = new HashMap<>(Map.of("dataset", "datasetindex_v2_old_456"));

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    DataHubUpgradeResultStore failing =
        new DataHubUpgradeResultStore() {
          @Override
          public EnvelopedAspect readLatest(OperationContext opContext, Urn urn) throws Exception {
            throw new IllegalStateException("GMS unreachable");
          }

          @Override
          public void ingest(OperationContext opContext, MetadataChangeProposal proposal) {
            throw new UnsupportedOperationException();
          }
        };

    strategy.reconcileTargets(operationContext, failing, upgradeIdUrn);

    assertTrue(strategy.isEnabled(), "targets must survive a failed state read");
  }

  @Test
  public void testRemoveTarget() {
    String nextIndex = "datasetindex_v2_next_123";
    Map<String, String> targets = new HashMap<>(Map.of("dataset", nextIndex));
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    assertTrue(strategy.isEnabled());

    strategy.removeTarget("dataset");

    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testTransformExceptionDoesNotPropagate() throws Exception {
    Map<String, String> targets = Map.of("dataset", "datasetindex_v2_next_123");
    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenThrow(new RuntimeException("transform error"));

    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(testUrn, List.of(mockEvent));

    // Should not throw — errors are logged and swallowed
    strategy.processBatch(operationContext, events, false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testReconcileRemovesSwappedTargets() throws Exception {
    Map<String, String> targets = new HashMap<>(Map.of("dataset", "datasetindex_v2_next_123"));
    Urn upgradeIdUrn = UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    assertTrue(strategy.isEnabled());

    DataHubUpgradeResultStore store =
        storeReturning(
            upgradeResultAspect(
                "datasetindex_v2",
                "datasetindex_v2_next_123",
                null,
                IncrementalReindexState.Status.DUAL_WRITE_DISABLED));

    // Invoke the poll directly
    strategy.reconcileTargets(operationContext, store, upgradeIdUrn);

    assertFalse(strategy.isEnabled());
  }

  /**
   * Reverse direction of the case mismatch: a registry-case key against a swap notification whose
   * entity name is derived from the lowercased index. The swap-cleanup filter has to bridge it too,
   * or a swapped index keeps receiving dual writes after dual-write is disabled.
   */
  @Test
  public void testReconcileRemovesSwappedTargetsForMixedCaseEntity() throws Exception {
    Map<String, String> targets = new HashMap<>(Map.of("dataFlow", "dataflowindex_v2_next_123"));
    Urn upgradeIdUrn = UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    assertTrue(strategy.isEnabled());

    DataHubUpgradeResultStore store =
        storeReturning(
            upgradeResultAspect(
                "dataflowindex_v2",
                "dataflowindex_v2_next_123",
                null,
                IncrementalReindexState.Status.DUAL_WRITE_DISABLED));

    strategy.reconcileTargets(operationContext, store, upgradeIdUrn);

    assertFalse(strategy.isEnabled());
  }

  /**
   * A real store rather than a Mockito mock, so the reconcile path parses actual aspect data
   * instead of a stubbed return value.
   */
  private static DataHubUpgradeResultStore storeReturning(EnvelopedAspect aspect) {
    return new DataHubUpgradeResultStore() {
      @Override
      public EnvelopedAspect readLatest(OperationContext opContext, Urn upgradeIdUrn) {
        return aspect;
      }

      @Override
      public void ingest(OperationContext opContext, MetadataChangeProposal proposal) {
        throw new UnsupportedOperationException("dual-write poller must not write");
      }
    };
  }

  private static EnvelopedAspect upgradeResultAspect(
      String indexName,
      String nextIndexName,
      String oldBackingIndexName,
      IncrementalReindexState.Status status) {
    Map<String, String> upgradeState =
        IncrementalReindexState.setPhase1State(
            null, indexName, nextIndexName, oldBackingIndexName, 100L, 0L, null, true, status);

    DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult();
    upgradeResult.setState(DataHubUpgradeState.SUCCEEDED);
    upgradeResult.setResult(new StringMap(upgradeState));

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(upgradeResult.data()));
    return envelopedAspect;
  }
}
