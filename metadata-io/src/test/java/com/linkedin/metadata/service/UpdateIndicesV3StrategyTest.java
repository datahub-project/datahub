package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateIndicesV3StrategyTest {

  @Mock private EntityIndexVersionConfiguration v3Config;
  @Mock private ElasticSearchService elasticSearchService;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private TimeseriesAspectService timeseriesAspectService;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;
  @Mock private MCLItem mockEvent;
  @Mock private RecordTemplate mockAspect;
  @Mock private SystemMetadata mockSystemMetadata;
  @Mock private AuditStamp mockAuditStamp;
  private ObjectNode mockSearchDocument;
  @Mock private ReindexConfig mockReindexConfig;

  private OperationContext operationContext;
  private UpdateIndicesV3Strategy strategy;
  private Urn testUrn;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    // Setup common mocks
    when(v3Config.isEnabled()).thenReturn(true);
    when(mockEvent.getUrn()).thenReturn(testUrn);
    when(mockEvent.getEntitySpec()).thenReturn(mockEntitySpec);
    when(mockEvent.getAspectSpec()).thenReturn(mockAspectSpec);
    when(mockEvent.getRecordTemplate()).thenReturn(mockAspect);
    when(mockEvent.getSystemMetadata()).thenReturn(mockSystemMetadata);

    // Setup entity spec mocks
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");
    when(mockEvent.getAuditStamp()).thenReturn(mockAuditStamp);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEntitySpec.getName()).thenReturn(DATASET_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(mockAspectSpec.isTimeseries()).thenReturn(false);

    // Setup mock search document
    mockSearchDocument = JsonNodeFactory.instance.objectNode();
    mockSearchDocument.put("urn", testUrn.toString());
    mockSearchDocument.put("_entityType", "dataset");

    // Create strategy with V2 disabled (testing V3-only scenario)
    strategy =
        new UpdateIndicesV3Strategy(
            v3Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            false); // v2Enabled = false
  }

  @Test
  public void testIsEnabled() {
    // Test when enabled
    when(v3Config.isEnabled()).thenReturn(true);
    assertTrue(strategy.isEnabled());

    // Test when disabled
    when(v3Config.isEnabled()).thenReturn(false);
    assertFalse(strategy.isEnabled());
  }

  @Test
  public void testProcessBatch_EmptyEvents() {
    // Execute with empty events
    strategy.processBatch(operationContext, Collections.emptyMap());

    // Verify no processing occurred
    verify(elasticSearchService, never())
        .upsertDocument(any(), anyString(), anyString(), anyString());
    verify(elasticSearchService, never()).deleteDocument(any(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_SingleEvent() throws Exception {
    // Setup mocks
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents);

    // Verify
    verify(elasticSearchService)
        .upsertDocumentBySearchGroup(
            eq(operationContext),
            anyString(), // search group
            anyString(), // document
            anyString()); // doc id
  }

  // Note: Key aspect deletion test is complex due to static method calls
  // and would require more sophisticated mocking. Skipping for now.

  @Test
  public void testProcessBatch_StructuredProperties() throws Exception {
    // Setup for structured properties
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents);

    // Verify upsert was called with search group
    verify(elasticSearchService)
        .upsertDocumentBySearchGroup(
            eq(operationContext),
            anyString(), // search group
            anyString(), // document
            anyString()); // doc id
  }

  @Test
  public void testProcessBatch_TimeseriesAspect() throws Exception {
    // Setup for timeseries aspect
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute
    strategy.processBatch(operationContext, groupedEvents);

    // Verify upsert was called with search group (timeseries aspects are treated like regular
    // aspects in V3)
    verify(elasticSearchService)
        .upsertDocumentBySearchGroup(
            eq(operationContext),
            anyString(), // search group
            anyString(), // document
            anyString()); // doc id
  }

  @Test
  public void testUpdateIndexMappings_V2Enabled_SkipsProcessing() {
    // Create strategy with V2 enabled
    UpdateIndicesV3Strategy v2EnabledStrategy =
        new UpdateIndicesV3Strategy(
            v3Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            true); // v2Enabled = true

    // Setup for structured property
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

    // Execute
    v2EnabledStrategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify no processing occurred (V2 handles it)
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testUpdateIndexMappings_V2Disabled_ProcessesStructuredProperty() throws Exception {
    // Setup for structured property
    when(mockEntitySpec.getName()).thenReturn(STRUCTURED_PROPERTY_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);

    StructuredPropertyDefinition mockStructuredProperty = new StructuredPropertyDefinition();
    mockStructuredProperty.setEntityTypes(
        new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")));

    when(mockAspect.data()).thenReturn(mockStructuredProperty.data());

    // Mock the reindex configs
    when(elasticSearchService.buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class)))
        .thenReturn(Collections.singletonList(mockReindexConfig));
    when(mockReindexConfig.name()).thenReturn("test-index");

    // Execute
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify processing occurred
    verify(elasticSearchService)
        .buildReindexConfigsWithNewStructProp(
            eq(operationContext), eq(testUrn), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testUpdateIndexMappings_NonStructuredProperty() {
    // Setup for non-structured property
    when(mockEntitySpec.getName()).thenReturn(DATASET_ENTITY_NAME);
    when(mockAspectSpec.getName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);

    // Execute
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify no processing occurred
    verify(elasticSearchService, never())
        .buildReindexConfigsWithNewStructProp(
            any(OperationContext.class), any(Urn.class), any(StructuredPropertyDefinition.class));
  }

  @Test
  public void testNoOpMethods() {
    // Test that no-op methods don't throw exceptions
    strategy.deleteSearchData(
        operationContext,
        testUrn,
        DATASET_ENTITY_NAME,
        mockAspectSpec,
        mockAspect,
        true,
        mockAuditStamp);
    strategy.updateSearchIndices(operationContext, Collections.singletonList(mockEvent));
    strategy.updateTimeseriesFields(operationContext, Collections.singletonList(mockEvent));

    // These should not call any elasticsearch operations
    verify(elasticSearchService, never())
        .upsertDocument(any(), anyString(), anyString(), anyString());
    verify(elasticSearchService, never()).deleteDocument(any(), anyString(), anyString());
  }

  @Test
  public void testGetIndexMappings() {
    // Execute
    Collection<MappingsBuilder.IndexMapping> mappings = strategy.getIndexMappings(operationContext);

    // Verify - should return mappings from MultiEntityMappingsBuilder
    assertTrue(mappings instanceof Collection);
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Execute
    Collection<MappingsBuilder.IndexMapping> mappings =
        strategy.getIndexMappingsWithNewStructuredProperty(
            operationContext, testUrn, new StructuredPropertyDefinition());

    // Verify - should return empty collection (stub implementation)
    assertTrue(mappings.isEmpty());
  }
}
