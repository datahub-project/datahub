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
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.utils.elasticsearch.EntityIndexVersion;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
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

  private static final String DATASET_INDEX = "datasetindex_v2";
  private static final String DATASET_OLD_BACKING = "datasetindex_v2_1779165194703";

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
  public void testEnabledOnlyWhileV2TargetsExist() {
    assertFalse(strategy(targets(Map.of())).isEnabled());
    assertTrue(strategy(targets(Map.of(DATASET_INDEX, DATASET_OLD_BACKING))).isEnabled());

    IncrementalReindexWriteTargets v3Only =
        new IncrementalReindexWriteTargets(
            Map.of(
                "primaryindex_v3",
                new IncrementalReindexWriteTargets.RollbackTarget(
                    "primaryindex_v3", "primaryindex_v3_1779165194703", EntityIndexVersion.V3)),
            Map.of(),
            null,
            null,
            true);
    assertFalse(strategy(v3Only).isEnabled());
  }

  @Test
  public void testProcessBatchNoOpWhenNoTargets() {
    UpdateIndicesUpgradeStrategy strategy = strategy(targets(Map.of()));

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatchWritesToOldBackingIndex() throws Exception {
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of(DATASET_INDEX, DATASET_OLD_BACKING)));
    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", testUrn.toString());
    searchDoc.put("name", "SampleHdfsDataset");
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService)
        .upsertDocumentByIndexName(
            eq(operationContext),
            eq(DATASET_OLD_BACKING),
            eq(searchDoc.toString()),
            eq(
                operationContext
                    .getSearchContext()
                    .getIndexConvention()
                    .getEntityDocumentId(testUrn)));
  }

  /**
   * Targets are keyed by the alias the index convention derives, which lowercases the entity name.
   * An entity whose registry name has capitals must still resolve its target through that alias.
   */
  @Test
  public void testProcessBatchResolvesMixedCaseEntityThroughItsAlias() throws Exception {
    String dataFlowOldBacking = "dataflowindex_v2_1779165194703";
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of("dataflowindex_v2", dataFlowOldBacking)));
    Urn dataFlowUrn = UrnUtils.getUrn("urn:li:dataFlow:(airflow,my_dag,PROD)");
    when(mockEvent.getUrn()).thenReturn(dataFlowUrn);
    when(mockEntitySpec.getName()).thenReturn("dataFlow");
    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", dataFlowUrn.toString());
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    strategy.processBatch(operationContext, events(dataFlowUrn, mockEvent), false);

    verify(elasticSearchService)
        .upsertDocumentByIndexName(
            eq(operationContext), eq(dataFlowOldBacking), eq(searchDoc.toString()), anyString());
  }

  @Test
  public void testProcessBatchSkipsEntityWithoutTarget() throws Exception {
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of("chartindex_v2", "chartindex_v2_1779165194703")));

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
    verify(searchDocumentTransformer, never())
        .transformAspect(any(), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void testProcessBatchSkipsEmptySearchDocument() throws Exception {
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of(DATASET_INDEX, DATASET_OLD_BACKING)));
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.empty());

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatchDeleteKeyAspect() {
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of(DATASET_INDEX, DATASET_OLD_BACKING)));
    when(mockMcl.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey");

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService)
        .deleteDocumentByIndexName(eq(operationContext), eq(DATASET_OLD_BACKING), anyString());
  }

  @Test
  public void testDualWriteStartRecordedForTheAliasOnce() throws Exception {
    AtomicLong capturedTime = new AtomicLong(0);
    AtomicReference<String> capturedIndex = new AtomicReference<>();
    IncrementalReindexWriteTargets targets =
        new IncrementalReindexWriteTargets(
            Map.of(DATASET_INDEX, v2Target(DATASET_INDEX, DATASET_OLD_BACKING)),
            Map.of(),
            (indexName, startTime) -> {
              capturedIndex.set(indexName);
              capturedTime.set(startTime);
            },
            null,
            true);
    UpdateIndicesUpgradeStrategy strategy = strategy(targets);
    ObjectNode searchDoc = JsonNodeFactory.instance.objectNode();
    searchDoc.put("urn", testUrn.toString());
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenReturn(Optional.of(searchDoc));

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);
    assertTrue(capturedTime.get() > 0);
    assertEquals(capturedIndex.get(), DATASET_INDEX);

    capturedTime.set(0);
    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);
    assertEquals(capturedTime.get(), 0L);
  }

  @Test
  public void testTransformExceptionDoesNotPropagate() throws Exception {
    UpdateIndicesUpgradeStrategy strategy =
        strategy(targets(Map.of(DATASET_INDEX, DATASET_OLD_BACKING)));
    when(searchDocumentTransformer.transformAspect(any(), any(), any(), any(), eq(false), any()))
        .thenThrow(new RuntimeException("transform error"));

    strategy.processBatch(operationContext, events(testUrn, mockEvent), false);

    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(
            any(OperationContext.class), anyString(), anyString(), anyString());
  }

  private UpdateIndicesUpgradeStrategy strategy(IncrementalReindexWriteTargets targets) {
    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService, searchDocumentTransformer, targets);
  }

  private static IncrementalReindexWriteTargets targets(Map<String, String> oldBackingByAlias) {
    Map<String, IncrementalReindexWriteTargets.RollbackTarget> targets = new LinkedHashMap<>();
    oldBackingByAlias.forEach(
        (alias, oldBacking) -> targets.put(alias, v2Target(alias, oldBacking)));
    return new IncrementalReindexWriteTargets(targets, Map.of(), null, null, true);
  }

  private static IncrementalReindexWriteTargets.RollbackTarget v2Target(
      String alias, String oldBacking) {
    return new IncrementalReindexWriteTargets.RollbackTarget(
        alias, oldBacking, EntityIndexVersion.V2);
  }

  private static Map<Urn, List<MCLItem>> events(Urn urn, MCLItem event) {
    LinkedHashMap<Urn, List<MCLItem>> events = new LinkedHashMap<>();
    events.put(urn, List.of(event));
    return events;
  }
}
