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
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
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
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
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
}
