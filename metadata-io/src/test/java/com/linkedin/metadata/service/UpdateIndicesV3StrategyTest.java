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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

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
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
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
  @Mock private ESIndexBuilder mockIndexBuilder;
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

    // Setup mock index builder
    when(elasticSearchService.getIndexBuilder()).thenReturn(mockIndexBuilder);

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
    strategy.processBatch(operationContext, Collections.emptyMap(), true);

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
    strategy.processBatch(operationContext, groupedEvents, true);

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
    strategy.processBatch(operationContext, groupedEvents, true);

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
    strategy.processBatch(operationContext, groupedEvents, true);

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

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigResourceNotFound() {
    // Setup v3Config to use a non-existent mapping configuration resource
    when(v3Config.getMappingConfig()).thenReturn("non-existent-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(
            RuntimeException.class,
            () ->
                new UpdateIndicesV3Strategy(
                    v3Config,
                    elasticSearchService,
                    searchDocumentTransformer,
                    timeseriesAspectService,
                    "MD5",
                    false));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
    assertTrue(exception.getCause().getMessage().contains("Configuration resource not found"));
  }

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigHasInvalidFormat() {
    // Setup v3Config to use a mapping configuration with invalid format
    when(v3Config.getMappingConfig()).thenReturn("invalid-format-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(
            RuntimeException.class,
            () ->
                new UpdateIndicesV3Strategy(
                    v3Config,
                    elasticSearchService,
                    searchDocumentTransformer,
                    timeseriesAspectService,
                    "MD5",
                    false));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
  }

  @Test
  public void testConstructor_IOExceptionWhenMappingConfigMissingRequiredSections() {
    // Setup v3Config to use a mapping configuration missing required sections
    when(v3Config.getMappingConfig()).thenReturn("missing-sections-mapping-config.yaml");

    // Execute and verify that RuntimeException is thrown with IOException as cause
    RuntimeException exception =
        expectThrows(
            RuntimeException.class,
            () ->
                new UpdateIndicesV3Strategy(
                    v3Config,
                    elasticSearchService,
                    searchDocumentTransformer,
                    timeseriesAspectService,
                    "MD5",
                    false));

    // Verify the exception message and cause
    assertTrue(exception.getMessage().contains("Failed to initialize V3 mappings builder"));
    assertTrue(exception.getCause() instanceof IOException);
    // Note: The specific error message may vary depending on the actual file content
    // We just verify that an IOException is thrown
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsNull() {
    // Setup v3Config with null mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn(null);

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithNullConfig =
        new UpdateIndicesV3Strategy(
            v3Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            false);

    // Verify strategy was created successfully
    assertTrue(strategyWithNullConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsEmpty() {
    // Setup v3Config with empty mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithEmptyConfig =
        new UpdateIndicesV3Strategy(
            v3Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            false);

    // Verify strategy was created successfully
    assertTrue(strategyWithEmptyConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsWhitespace() {
    // Setup v3Config with whitespace-only mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("   ");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithWhitespaceConfig =
        new UpdateIndicesV3Strategy(
            v3Config,
            elasticSearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            false);

    // Verify strategy was created successfully
    assertTrue(strategyWithWhitespaceConfig.isEnabled());
  }

  @Test
  public void testUpdateIndexMappings_IOExceptionFromApplyMappings() throws Exception {
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

    // Mock applyMappings to throw IOException
    doThrow(new IOException("Elasticsearch communication error"))
        .when(mockIndexBuilder)
        .applyMappings(any(ReindexConfig.class), anyBoolean());

    // Execute - the method catches the RuntimeException and logs it, so no exception is thrown
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify that applyMappings was called (which would have thrown the IOException)
    verify(mockIndexBuilder).applyMappings(any(ReindexConfig.class), eq(false));
  }

  @Test
  public void testProcessBatch_ExceptionInKeyAspectDeletionCheck() throws Exception {
    // Setup for DELETE event
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey");

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil.extractSpecPair to throw RuntimeException
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenThrow(new RuntimeException("Failed to retrieve Aspect Spec"));

      // Execute - the method should handle the exception gracefully
      strategy.processBatch(operationContext, groupedEvents, true);

      // Verify that no document operations were performed due to the exception
      verify(elasticSearchService, never())
          .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
      verify(elasticSearchService, never()).deleteDocument(any(), anyString(), anyString());
    }
  }

  @Test
  public void testProcessBatch_KeyAspectDeletion_Success() throws Exception {
    // Setup for DELETE event with key aspect
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey"); // This is the key aspect
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil methods to return successful results
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenReturn(Pair.of(mockEntitySpec, mockAspectSpec));
      mockedStatic.when(() -> UpdateIndicesUtil.isDeletingKey(any(Pair.class))).thenReturn(true);

      // Execute
      strategy.processBatch(operationContext, groupedEvents, true);

      // Verify that deleteDocumentBySearchGroup was called
      verify(elasticSearchService)
          .deleteDocumentBySearchGroup(eq(operationContext), eq("dataset"), anyString());
    }
  }

  @Test
  public void testProcessBatch_KeyAspectDeletion_NullSearchGroup() throws Exception {
    // Setup for DELETE event with key aspect but null search group
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    when(mockEvent.getAspectName()).thenReturn("datasetKey"); // This is the key aspect
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn(null); // Null search group

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock UpdateIndicesUtil methods to return successful results
    try (var mockedStatic = mockStatic(UpdateIndicesUtil.class)) {
      mockedStatic
          .when(() -> UpdateIndicesUtil.extractSpecPair(any(MCLItem.class)))
          .thenReturn(Pair.of(mockEntitySpec, mockAspectSpec));
      mockedStatic.when(() -> UpdateIndicesUtil.isDeletingKey(any(Pair.class))).thenReturn(true);

      // Execute - should handle null search group gracefully
      strategy.processBatch(operationContext, groupedEvents, true);

      // Verify that no delete operation was performed due to null search group
      verify(elasticSearchService, never())
          .deleteDocumentBySearchGroup(any(), anyString(), anyString());
    }
  }

  @Test
  public void testProcessBatch_EmptyEventsListForUrn() throws Exception {
    // Setup for URN with empty events list (different from empty groupedEvents map)
    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.emptyList());

    // Execute and verify that IndexOutOfBoundsException is thrown due to bug in
    // buildV3SearchDocument
    // This test documents the current behavior - the code crashes when events list is empty
    // because buildV3SearchDocument tries to access events.get(0) without checking if events is
    // empty
    IndexOutOfBoundsException exception =
        expectThrows(
            IndexOutOfBoundsException.class,
            () -> strategy.processBatch(operationContext, groupedEvents, true));

    // Verify the exception message indicates the issue
    assertTrue(exception.getMessage().contains("Index: 0"));
  }

  @Test
  public void testProcessBatch_UpsertWithNullSearchGroup() throws Exception {
    // Setup for UPSERT event with null search group
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn(null); // Null search group

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Mock search document transformer to return a document
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    // Execute - should handle null search group gracefully
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify that no upsert operation was performed due to null search group
    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_ExceptionInAspectProcessing() throws Exception {
    // Setup for UPSERT event that will cause an exception during aspect processing
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    // Mock getAspectSpec to throw an exception
    when(mockEvent.getAspectSpec()).thenThrow(new RuntimeException("Aspect spec error"));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute - should handle the exception gracefully and continue processing
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify that no upsert operation was performed due to the exception
    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_ExceptionInTransformAspect() throws Exception {
    // Setup for UPSERT event that will cause an exception during aspect transformation
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn("datasetProperties");
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    // Mock searchDocumentTransformer.transformAspect to throw an exception
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenThrow(new RuntimeException("Transform aspect error"));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute - should handle the exception gracefully and continue processing
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify that no upsert operation was performed due to the exception
    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }

  @Test
  public void testProcessBatch_ExceptionInStructuredPropertiesProcessing() throws Exception {
    // Setup for UPSERT event with structured properties aspect that will cause an exception
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getAspectName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getSearchGroup()).thenReturn("dataset");

    // Mock getAspectSpec to throw an exception during structured properties processing
    when(mockEvent.getAspectSpec()).thenThrow(new RuntimeException("Aspect spec error"));

    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent));

    // Execute - should handle the exception gracefully and continue processing
    strategy.processBatch(operationContext, groupedEvents, true);

    // Verify that no upsert operation was performed due to the exception
    verify(elasticSearchService, never())
        .upsertDocumentBySearchGroup(any(), anyString(), anyString(), anyString());
  }
}
