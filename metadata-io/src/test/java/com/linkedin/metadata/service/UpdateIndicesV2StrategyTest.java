package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateIndicesV2StrategyTest {

  @Mock private EntityIndexVersionConfiguration v2Config;
  @Mock private ElasticSearchService elasticSearchService;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private TimeseriesAspectService timeseriesAspectService;
  @Mock private SystemMetadataService systemMetadataService;
  @Mock private MCLItem mockEvent;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;
  @Mock private RecordTemplate mockAspect;
  @Mock private RecordTemplate mockPreviousAspect;
  @Mock private SystemMetadata mockSystemMetadata;
  @Mock private AuditStamp mockAuditStamp;
  @Mock private ObjectNode mockSearchDocument;
  @Mock private ObjectNode mockPreviousSearchDocument;
  @Mock private StructuredPropertyDefinition mockStructuredProperty;

  private OperationContext operationContext;
  private UpdateIndicesV2Strategy strategy;
  private Urn testUrn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    // Setup default mocks
    when(v2Config.isEnabled()).thenReturn(true);
    when(mockEvent.getUrn()).thenReturn(testUrn);
    when(mockEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockEvent.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockEvent.getRecordTemplate()).thenReturn(mockAspect);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(mockPreviousAspect);
    when(mockEvent.getSystemMetadata()).thenReturn(mockSystemMetadata);
    when(mockEvent.getAuditStamp()).thenReturn(mockAuditStamp);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    // Setup RecordTemplate mocks
    DataMap mockDataMap = mock(DataMap.class);
    when(mockDataMap.containsKey(any())).thenReturn(false);
    when(mockAspect.data()).thenReturn(mockDataMap);
    when(mockPreviousAspect.data()).thenReturn(mockDataMap);

    // Setup StructuredPropertyDefinition mock
    com.linkedin.common.UrnArray mockUrnArray = mock(com.linkedin.common.UrnArray.class);
    when(mockUrnArray.contains(any())).thenReturn(false);
    when(mockStructuredProperty.getEntityTypes()).thenReturn(mockUrnArray);

    strategy =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            null, // No semantic search config for basic tests
            mock(IndexConvention.class));
  }

  @Test
  public void testIsEnabled() {
    // Test when enabled
    when(v2Config.isEnabled()).thenReturn(true);
    assertTrue(strategy.isEnabled());

    // Test when disabled
    when(v2Config.isEnabled()).thenReturn(false);
    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testUpdateSearchIndices_Success() throws Exception {
    // Setup mocks
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false), // Use eq(false) instead of anyBoolean()
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");

    // Mock the withSystemCreated method behavior by ensuring the document is not empty
    when(mockSearchDocument.isEmpty()).thenReturn(false);

    // Mock the previous search document to be different to avoid diff mode skip
    ObjectNode mockPreviousSearchDocument = mock(ObjectNode.class);
    when(mockPreviousSearchDocument.toString()).thenReturn("{\"previous\": \"document\"}");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockPreviousAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(mockPreviousSearchDocument));

    // Execute
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    // Verify
    verify(searchDocumentTransformer)
        .transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp));
    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testUpdateSearchIndices_CallsAppendRunIdWhenRunIdPresent() throws Exception {
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-123");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");
    when(mockSearchDocument.isEmpty()).thenReturn(false);

    ObjectNode mockPreviousSearchDocument = mock(ObjectNode.class);
    when(mockPreviousSearchDocument.toString()).thenReturn("{\"previous\": \"document\"}");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockPreviousAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(mockPreviousSearchDocument));

    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
    verify(elasticSearchService).appendRunId(eq(operationContext), eq(testUrn), eq("run-123"));
  }

  @Test
  public void testUpdateSearchIndices_EmptySearchDocument() throws Exception {
    // Setup mocks - return empty search document
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.empty());

    // Execute
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    // Verify - should not call upsertDocument
    verify(elasticSearchService, never())
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testUpdateSearchIndices_WithForceIndexing() throws Exception {
    // Setup mocks with FORCE_INDEXING
    StringMap properties = new StringMap();
    properties.put("FORCE_INDEXING", "true");
    when(mockSystemMetadata.getProperties()).thenReturn(properties);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");

    // Execute
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    // Verify - should call upsertDocument even with same content due to FORCE_INDEXING
    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testUpdateSearchIndices_TransformException() throws Exception {
    // Setup mocks to throw exception
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenThrow(new RuntimeException("Transform error"));

    // Execute
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    // Verify - should not call upsertDocument due to exception
    verify(elasticSearchService, never())
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testUpdateTimeseriesFields_NonTimeseriesAspect() throws Exception {
    // Setup mocks - non-timeseries aspect
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    // Execute
    strategy.updateTimeseriesFields(operationContext, Collections.singletonList(mockEvent));

    // Verify timeseries aspect service was not called
    verify(timeseriesAspectService, never())
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString(), any());
  }

  @Test
  public void testDeleteSearchData_KeyAspect() throws Exception {
    // Execute with isKeyAspect = true
    strategy.deleteSearchData(
        operationContext,
        testUrn,
        "dataset",
        mockAspectSpec,
        mockAspect,
        true, // isKeyAspect
        mockAuditStamp);

    // Verify deleteDocument was called
    verify(elasticSearchService).deleteDocument(eq(operationContext), eq("dataset"), anyString());
    verify(elasticSearchService, never())
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testDeleteSearchData_NonKeyAspect() throws Exception {
    // Setup mocks
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");

    // Execute
    strategy.deleteSearchData(
        operationContext,
        testUrn,
        "dataset",
        mockAspectSpec,
        mockAspect,
        false, // isKeyAspect
        mockAuditStamp);

    // Verify upsertDocument was called (not deleteDocument)
    verify(elasticSearchService, never())
        .deleteDocument(any(OperationContext.class), anyString(), anyString());
    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testDeleteSearchData_EmptySearchDocument() throws Exception {
    // Setup mocks - return empty search document
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.empty());

    // Execute
    strategy.deleteSearchData(
        operationContext,
        testUrn,
        "dataset",
        mockAspectSpec,
        mockAspect,
        false, // isKeyAspect
        mockAuditStamp);

    // Verify - should not call any elasticsearch operations
    verify(elasticSearchService, never())
        .deleteDocument(any(OperationContext.class), anyString(), anyString());
    verify(elasticSearchService, never())
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString());
  }

  @Test
  public void testUpdateIndexMappings_NonStructuredProperty() throws Exception {
    // Setup mocks for non-structured property
    when(mockEntitySpec.getName()).thenReturn("dataset");
    when(mockAspectSpec.getName()).thenReturn("datasetProperties");

    // Execute
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify elasticsearch service was not called
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testGetIndexMappings() {
    // Execute
    Collection<MappingsBuilder.IndexMapping> mappings = strategy.getIndexMappings(operationContext);

    // Verify - should return mappings from LegacyMappingsBuilder
    assertTrue(mappings instanceof Collection);
    // Note: The actual content depends on the LegacyMappingsBuilder implementation
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Execute
    Collection<MappingsBuilder.IndexMapping> mappings =
        strategy.getIndexMappingsWithNewStructuredProperty(
            operationContext, testUrn, mockStructuredProperty);

    // Verify - should return mappings from LegacyMappingsBuilder
    assertTrue(mappings instanceof Collection);
    // Note: The actual content depends on the LegacyMappingsBuilder implementation
  }

  // ==================== Semantic Search Dual-Write Tests ====================

  @Test
  public void testShouldWriteToSemanticIndex_SemanticSearchDisabled() {
    // Strategy created without semantic config should not write to semantic index
    assertFalse(strategy.shouldWriteToSemanticIndex(operationContext, "dataset"));
  }

  @Test
  public void testShouldWriteToSemanticIndex_SemanticSearchNotEnabled() {
    // Setup: Create semantic config with enabled=false
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(false);
    IndexConvention indexConvention = mock(IndexConvention.class);

    UpdateIndicesV2Strategy strategyWithDisabledSemantic =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Verify: Should not write to semantic index when not enabled
    assertFalse(
        strategyWithDisabledSemantic.shouldWriteToSemanticIndex(operationContext, "dataset"));
  }

  @Test
  public void testShouldWriteToSemanticIndex_EntityNotInEnabledList() {
    // Setup: Create semantic config with enabled=true but entity not in list
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("document")); // Only document
    IndexConvention indexConvention = mock(IndexConvention.class);

    UpdateIndicesV2Strategy strategyWithLimitedEntities =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Verify: Should not write to semantic index for dataset (not in enabled list)
    assertFalse(
        strategyWithLimitedEntities.shouldWriteToSemanticIndex(operationContext, "dataset"));
  }

  @Test
  public void testShouldWriteToSemanticIndex_IndexDoesNotExist() {
    // Setup: Semantic config enabled, entity in list, but index doesn't exist
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("dataset"));
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexNameSemantic("dataset"))
        .thenReturn("datasetindex_v2_semantic");
    when(elasticSearchService.indexExists("datasetindex_v2_semantic")).thenReturn(false);

    UpdateIndicesV2Strategy strategyWithMissingIndex =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Verify: Should not write to semantic index when index doesn't exist
    assertFalse(strategyWithMissingIndex.shouldWriteToSemanticIndex(operationContext, "dataset"));
  }

  @Test
  public void testShouldWriteToSemanticIndex_AllConditionsMet() {
    // Setup: All conditions met
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("dataset"));
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexNameSemantic("dataset"))
        .thenReturn("datasetindex_v2_semantic");
    when(elasticSearchService.indexExists("datasetindex_v2_semantic")).thenReturn(true);

    UpdateIndicesV2Strategy strategyWithAllConditions =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Verify: Should write to semantic index when all conditions are met
    assertTrue(strategyWithAllConditions.shouldWriteToSemanticIndex(operationContext, "dataset"));
  }

  @Test
  public void testShouldWriteToSemanticIndex_CachesIndexExistsResult() {
    // Setup: All conditions met
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("dataset"));
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexNameSemantic("dataset"))
        .thenReturn("datasetindex_v2_semantic");
    when(elasticSearchService.indexExists("datasetindex_v2_semantic")).thenReturn(true);

    UpdateIndicesV2Strategy strategyWithCaching =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Call shouldWriteToSemanticIndex multiple times
    assertTrue(strategyWithCaching.shouldWriteToSemanticIndex(operationContext, "dataset"));
    assertTrue(strategyWithCaching.shouldWriteToSemanticIndex(operationContext, "dataset"));
    assertTrue(strategyWithCaching.shouldWriteToSemanticIndex(operationContext, "dataset"));

    // Verify: indexExists should only be called once due to caching
    verify(elasticSearchService, times(1)).indexExists("datasetindex_v2_semantic");
  }

  @Test
  public void testUpdateSearchIndices_DualWriteToSemanticIndex() throws Exception {
    // Setup: Create strategy with semantic search enabled
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("dataset"));
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexNameSemantic("dataset"))
        .thenReturn("datasetindex_v2_semantic");
    when(elasticSearchService.indexExists("datasetindex_v2_semantic")).thenReturn(true);

    UpdateIndicesV2Strategy strategyWithSemantic =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Setup mocks for document transformation
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");
    when(mockSearchDocument.isEmpty()).thenReturn(false);

    // Mock the previous search document to be different to avoid diff mode skip
    ObjectNode mockPreviousDoc = mock(ObjectNode.class);
    when(mockPreviousDoc.toString()).thenReturn("{\"previous\": \"document\"}");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockPreviousAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(mockPreviousDoc));

    // Execute
    strategyWithSemantic.updateSearchIndices(
        operationContext, Collections.singletonList(mockEvent));

    // Verify: Both V2 and semantic index should be written to
    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
    verify(elasticSearchService)
        .upsertDocumentByIndexName(eq("datasetindex_v2_semantic"), anyString(), anyString());
  }

  @Test
  public void testUpdateSearchIndices_NoSemanticWriteWhenDisabled() throws Exception {
    // Use the default strategy without semantic search
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
    when(mockSearchDocument.toString()).thenReturn("{\"test\": \"document\"}");
    when(mockSearchDocument.isEmpty()).thenReturn(false);

    // Mock the previous search document to be different
    ObjectNode mockPreviousDoc = mock(ObjectNode.class);
    when(mockPreviousDoc.toString()).thenReturn("{\"previous\": \"document\"}");
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(testUrn),
            eq(mockPreviousAspect),
            eq(mockAspectSpec),
            eq(false),
            eq(mockAuditStamp)))
        .thenReturn(Optional.of(mockPreviousDoc));

    // Execute
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));

    // Verify: Only V2 index should be written to (no semantic write)
    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
    verify(elasticSearchService, never())
        .upsertDocumentByIndexName(anyString(), anyString(), anyString());
  }

  @Test
  public void testDeleteSearchData_DualDeleteFromSemanticIndex() throws Exception {
    // Setup: Create strategy with semantic search enabled
    SemanticSearchConfiguration semanticConfig = mock(SemanticSearchConfiguration.class);
    when(semanticConfig.isEnabled()).thenReturn(true);
    when(semanticConfig.getEnabledEntities()).thenReturn(Set.of("dataset"));
    IndexConvention indexConvention = mock(IndexConvention.class);
    when(indexConvention.getEntityIndexNameSemantic("dataset"))
        .thenReturn("datasetindex_v2_semantic");
    when(elasticSearchService.indexExists("datasetindex_v2_semantic")).thenReturn(true);

    UpdateIndicesV2Strategy strategyWithSemantic =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            semanticConfig,
            indexConvention);

    // Execute with isKeyAspect = true (full delete)
    strategyWithSemantic.deleteSearchData(
        operationContext,
        testUrn,
        "dataset",
        mockAspectSpec,
        mockAspect,
        true, // isKeyAspect
        mockAuditStamp);

    // Verify: Both V2 and semantic index should have documents deleted
    verify(elasticSearchService).deleteDocument(eq(operationContext), eq("dataset"), anyString());
    verify(elasticSearchService)
        .deleteDocumentByIndexName(eq("datasetindex_v2_semantic"), anyString());
  }

  // ==================== processBatch coalescing tests ====================

  private MCLItem makeEvent(
      String aspectName,
      AspectSpec aspectSpec,
      RecordTemplate aspect,
      RecordTemplate previousAspect,
      String runId,
      ChangeType changeType) {
    MCLItem event = mock(MCLItem.class);
    when(event.getUrn()).thenReturn(testUrn);
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getEntitySpec()).thenReturn(mockEntitySpec);
    when(event.getRecordTemplate()).thenReturn(aspect);
    when(event.getPreviousRecordTemplate()).thenReturn(previousAspect);
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(changeType);

    SystemMetadata sm = mock(SystemMetadata.class);
    if (runId != null) {
      when(sm.hasRunId()).thenReturn(true);
      when(sm.getRunId()).thenReturn(runId);
    }
    when(event.getSystemMetadata()).thenReturn(sm);

    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    when(mcl.getChangeType()).thenReturn(changeType);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    return event;
  }

  private Map<Urn, List<MCLItem>> groupedFor(List<MCLItem> events) {
    Map<Urn, List<MCLItem>> grouped = new LinkedHashMap<>();
    grouped.put(testUrn, events);
    return grouped;
  }

  @Test
  public void testProcessBatch_CoalesceSameUrnSameAspect_OnlyLatestUpserted() throws Exception {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    when(ownershipSpec.isTimeseries()).thenReturn(false);

    RecordTemplate aspectA = mock(RecordTemplate.class);
    RecordTemplate aspectB = mock(RecordTemplate.class);

    MCLItem first =
        makeEvent("ownership", ownershipSpec, aspectA, null, "run-1", ChangeType.UPSERT);
    MCLItem second =
        makeEvent("ownership", ownershipSpec, aspectB, aspectA, "run-1", ChangeType.UPSERT);

    ObjectNode survivorDoc = mock(ObjectNode.class);
    when(survivorDoc.toString()).thenReturn("{\"v\":2}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(survivorDoc));

    strategy.processBatch(operationContext, groupedFor(List.of(first, second)), false);

    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(aspectB),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
    verify(searchDocumentTransformer, never())
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(aspectA),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
    verify(elasticSearchService, times(1))
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_CoalesceSameUrnDifferentAspects_BothProcessed() throws Exception {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    when(ownershipSpec.isTimeseries()).thenReturn(false);
    AspectSpec tagsSpec = mock(AspectSpec.class);
    when(tagsSpec.getName()).thenReturn("globalTags");
    when(tagsSpec.isTimeseries()).thenReturn(false);

    RecordTemplate ownershipAspect = mock(RecordTemplate.class);
    RecordTemplate tagsAspect = mock(RecordTemplate.class);

    MCLItem ownershipEvent =
        makeEvent("ownership", ownershipSpec, ownershipAspect, null, "run-1", ChangeType.UPSERT);
    MCLItem tagsEvent =
        makeEvent("globalTags", tagsSpec, tagsAspect, null, "run-1", ChangeType.UPSERT);

    ObjectNode doc = mock(ObjectNode.class);
    when(doc.toString()).thenReturn("{\"k\":1}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(doc));

    strategy.processBatch(operationContext, groupedFor(List.of(ownershipEvent, tagsEvent)), false);

    verify(elasticSearchService, times(2))
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_TimeseriesNotCoalesced() throws Exception {
    AspectSpec tsSpec = mock(AspectSpec.class);
    when(tsSpec.getName()).thenReturn("datasetProfile");
    when(tsSpec.isTimeseries()).thenReturn(true);
    when(tsSpec.getTimeseriesFieldSpecs()).thenReturn(Collections.emptyList());
    when(tsSpec.getTimeseriesFieldCollectionSpecs()).thenReturn(Collections.emptyList());

    DataMap data1 = new DataMap();
    data1.put("timestampMillis", 1L);
    DataMap data2 = new DataMap();
    data2.put("timestampMillis", 2L);
    RecordTemplate ts1 = mock(RecordTemplate.class);
    RecordTemplate ts2 = mock(RecordTemplate.class);
    when(ts1.data()).thenReturn(data1);
    when(ts2.data()).thenReturn(data2);

    MCLItem first = makeEvent("datasetProfile", tsSpec, ts1, null, "run-1", ChangeType.UPSERT);
    MCLItem second = makeEvent("datasetProfile", tsSpec, ts2, null, "run-1", ChangeType.UPSERT);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.empty());

    strategy.processBatch(operationContext, groupedFor(List.of(first, second)), false);

    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(ts1),
            eq(tsSpec),
            eq(false),
            any(AuditStamp.class));
    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(ts2),
            eq(tsSpec),
            eq(false),
            any(AuditStamp.class));
    verify(timeseriesAspectService, times(2))
        .upsertDocument(any(OperationContext.class), anyString(), anyString(), anyString(), any());
  }

  @Test
  public void testProcessBatch_CoalesceAppendsRunIdsFromPredecessors() throws Exception {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    when(ownershipSpec.isTimeseries()).thenReturn(false);

    RecordTemplate aspectA = mock(RecordTemplate.class);
    RecordTemplate aspectB = mock(RecordTemplate.class);

    MCLItem first =
        makeEvent("ownership", ownershipSpec, aspectA, null, "run-A", ChangeType.UPSERT);
    MCLItem second =
        makeEvent("ownership", ownershipSpec, aspectB, aspectA, "run-B", ChangeType.UPSERT);

    ObjectNode doc = mock(ObjectNode.class);
    when(doc.toString()).thenReturn("{\"v\":1}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(doc));

    strategy.processBatch(operationContext, groupedFor(List.of(first, second)), false);

    verify(elasticSearchService).appendRunId(eq(operationContext), eq(testUrn), eq("run-B"));
    verify(elasticSearchService).appendRunId(eq(operationContext), eq(testUrn), eq("run-A"));
  }

  @Test
  public void testProcessBatch_FlagDisabled_PreservesPerEventBehavior() throws Exception {
    UpdateIndicesV2Strategy legacyStrategy =
        new UpdateIndicesV2Strategy(
            v2Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            null,
            mock(IndexConvention.class),
            false);

    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    when(ownershipSpec.isTimeseries()).thenReturn(false);

    RecordTemplate aspectA = mock(RecordTemplate.class);
    RecordTemplate aspectB = mock(RecordTemplate.class);

    // previousRecordTemplate=null on both events skips the per-event diff short-circuit so we can
    // observe pure per-event upsert behavior under coalesceBatchUpdates=false.
    MCLItem first =
        makeEvent("ownership", ownershipSpec, aspectA, null, "run-1", ChangeType.UPSERT);
    MCLItem second =
        makeEvent("ownership", ownershipSpec, aspectB, null, "run-1", ChangeType.UPSERT);

    ObjectNode doc = mock(ObjectNode.class);
    when(doc.toString()).thenReturn("{\"v\":1}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(doc));

    legacyStrategy.processBatch(operationContext, groupedFor(List.of(first, second)), false);

    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(aspectA),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(aspectB),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
    verify(elasticSearchService, times(2))
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_CoalesceUsesOldestPredecessorAsDiffBaseline() throws Exception {
    AspectSpec ownershipSpec = mock(AspectSpec.class);
    when(ownershipSpec.getName()).thenReturn("ownership");
    when(ownershipSpec.isTimeseries()).thenReturn(false);

    RecordTemplate baselineRec = mock(RecordTemplate.class);
    RecordTemplate intermediate = mock(RecordTemplate.class);
    RecordTemplate finalRec = mock(RecordTemplate.class);

    MCLItem first =
        makeEvent(
            "ownership", ownershipSpec, intermediate, baselineRec, "run-1", ChangeType.UPSERT);
    MCLItem second =
        makeEvent("ownership", ownershipSpec, finalRec, intermediate, "run-1", ChangeType.UPSERT);
    MCLItem third =
        makeEvent("ownership", ownershipSpec, finalRec, finalRec, "run-1", ChangeType.UPSERT);

    ObjectNode finalDoc = mock(ObjectNode.class);
    when(finalDoc.toString()).thenReturn("{\"v\":\"final\"}");
    ObjectNode baselineDoc = mock(ObjectNode.class);
    when(baselineDoc.toString()).thenReturn("{\"v\":\"base\"}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(finalRec),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(finalDoc));
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(baselineRec),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(baselineDoc));

    strategy.processBatch(operationContext, groupedFor(List.of(first, second, third)), false);

    verify(elasticSearchService)
        .upsertDocument(eq(operationContext), eq("dataset"), anyString(), anyString());
    verify(searchDocumentTransformer)
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(baselineRec),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
    verify(searchDocumentTransformer, never())
        .transformAspect(
            any(OperationContext.class),
            eq(testUrn),
            eq(intermediate),
            eq(ownershipSpec),
            eq(false),
            any(AuditStamp.class));
  }

  @Test
  public void testProcessBatch_CoalesceStructuredPropertyMappingsUseOldestPredecessorBaseline()
      throws Exception {
    // Two MCLs in the same batch on the same structured-property URN:
    //   MCL 1: prev entityTypes [] -> new [X]
    //   MCL 2 (survivor): prev [X] -> new [X, Y]
    // Pre-coalesce, X's mapping was applied by MCL 1 and Y's by MCL 2. Under coalesce we fire
    // updateIndexMappings only for the survivor, so the diff baseline must be the oldest
    // predecessor's previousRecordTemplate ([]) — not the survivor's previous ([X]) — otherwise
    // X's mapping is silently dropped.
    AspectSpec spdSpec = mock(AspectSpec.class);
    when(spdSpec.getName()).thenReturn(Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
    when(spdSpec.isTimeseries()).thenReturn(false);
    EntitySpec spEntitySpec = mock(EntitySpec.class);
    when(spEntitySpec.getName()).thenReturn(Constants.STRUCTURED_PROPERTY_ENTITY_NAME);

    Urn typeX = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    Urn typeY = UrnUtils.getUrn("urn:li:entityType:datahub.dashboard");

    StructuredPropertyDefinition baselineDef =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("foo")
            .setEntityTypes(new UrnArray())
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition intermediateDef =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("foo")
            .setEntityTypes(new UrnArray(typeX))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));
    StructuredPropertyDefinition survivorDef =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("foo")
            .setEntityTypes(new UrnArray(typeX, typeY))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    MCLItem first =
        makeStructuredPropertyEvent(
            spEntitySpec, spdSpec, intermediateDef, baselineDef, "run-1", ChangeType.UPSERT);
    MCLItem second =
        makeStructuredPropertyEvent(
            spEntitySpec, spdSpec, survivorDef, intermediateDef, "run-1", ChangeType.UPSERT);

    ObjectNode doc = mock(ObjectNode.class);
    when(doc.toString()).thenReturn("{\"v\":1}");
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            eq(false),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(doc));
    when(elasticSearchService.buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class)))
        .thenReturn(Collections.emptyList());

    strategy.processBatch(operationContext, groupedFor(List.of(first, second)), true);

    org.mockito.ArgumentCaptor<StructuredPropertyDefinition> defCaptor =
        org.mockito.ArgumentCaptor.forClass(StructuredPropertyDefinition.class);
    verify(elasticSearchService)
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), eq(testUrn), defCaptor.capture());

    UrnArray applied = defCaptor.getValue().getEntityTypes();
    assertTrue(
        applied.contains(typeX),
        "Coalesced mapping diff dropped entityType X added by an earlier MCL in the batch");
    assertTrue(applied.contains(typeY), "Coalesced mapping diff missing newly-added entityType Y");
  }

  private MCLItem makeStructuredPropertyEvent(
      EntitySpec entitySpec,
      AspectSpec aspectSpec,
      StructuredPropertyDefinition aspect,
      StructuredPropertyDefinition previousAspect,
      String runId,
      ChangeType changeType) {
    String aspectName = aspectSpec.getName();
    MCLItem event = mock(MCLItem.class);
    when(event.getUrn()).thenReturn(testUrn);
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getEntitySpec()).thenReturn(entitySpec);
    when(event.getRecordTemplate()).thenReturn(aspect);
    when(event.getPreviousRecordTemplate()).thenReturn(previousAspect);
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(changeType);

    SystemMetadata sm = mock(SystemMetadata.class);
    if (runId != null) {
      when(sm.hasRunId()).thenReturn(true);
      when(sm.getRunId()).thenReturn(runId);
    }
    when(event.getSystemMetadata()).thenReturn(sm);

    MetadataChangeLog mcl = mock(MetadataChangeLog.class);
    when(mcl.getChangeType()).thenReturn(changeType);
    when(event.getMetadataChangeLog()).thenReturn(mcl);
    return event;
  }
}
