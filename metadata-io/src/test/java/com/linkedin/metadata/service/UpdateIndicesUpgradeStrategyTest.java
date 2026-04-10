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
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.mxe.MetadataChangeLog;
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
        .upsertDocumentByIndexName(anyString(), anyString(), anyString());
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
        .upsertDocumentByIndexName(eq(nextIndex), eq(searchDoc.toString()), anyString());
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
        .upsertDocumentByIndexName(anyString(), anyString(), anyString());
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
        .upsertDocumentByIndexName(anyString(), anyString(), anyString());
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

    verify(elasticSearchService).deleteDocumentByIndexName(eq(nextIndex), anyString());
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
        .upsertDocumentByIndexName(anyString(), anyString(), anyString());
  }

  @Test
  public void testPollRemovesSwappedTargets() throws Exception {
    Map<String, String> targets = new HashMap<>(Map.of("dataset", "datasetindex_v2_next_123"));
    EntityService<?> mockEntityService = mock(EntityService.class);
    Urn upgradeIdUrn = UrnUtils.getUrn("urn:li:dataHubUpgrade:BuildIndicesIncremental_test");

    UpdateIndicesUpgradeStrategy strategy =
        new UpdateIndicesUpgradeStrategy(
            elasticSearchService, searchDocumentTransformer, targets, null, null, null, null, 0);

    assertTrue(strategy.isEnabled());

    // Build upgrade state with DUAL_WRITE_DISABLED for datasetindex_v2
    Map<String, String> upgradeState =
        IncrementalReindexState.setPhase1State(
            null,
            "datasetindex_v2",
            "datasetindex_v2_next_123",
            null,
            100L,
            true,
            IncrementalReindexState.Status.DUAL_WRITE_DISABLED);

    DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult();
    upgradeResult.setState(DataHubUpgradeState.SUCCEEDED);
    upgradeResult.setResult(new StringMap(upgradeState));

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(upgradeResult.data()));
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("dataHubUpgradeResult", envelopedAspect);
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);

    when(mockEntityService.getEntityV2(any(), any(), eq(upgradeIdUrn), any()))
        .thenReturn(entityResponse);

    // Invoke the poll directly
    strategy.pollForSwappedIndices(operationContext, mockEntityService, upgradeIdUrn);

    assertFalse(strategy.isEnabled());
  }
}
