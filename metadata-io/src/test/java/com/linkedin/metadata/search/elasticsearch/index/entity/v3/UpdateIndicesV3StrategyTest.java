package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.FORCE_INDEXING_KEY;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.StructuredPropertyMappingCache;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.TimeseriesWriteThrottleCache;
import com.linkedin.metadata.service.UpdateIndicesUtil;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.commons.codec.digest.DigestUtils;
import org.mockito.ArgumentCaptor;
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
    when(elasticSearchService.getIndexBuilder(anyString())).thenReturn(mockIndexBuilder);

    // Create strategy with V2 disabled (testing V3-only scenario)
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .build();
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

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    V3SearchDocumentWrite documentCaptorWrite = v3Write(operationContext, null, DOCUMENT_KINDS);
    assertEquals(documentCaptorWrite.docId(), expectedDocId);
    assertTrue(documentCaptorWrite.source().contains("\"urn\":\"" + testUrn + "\""));
    assertFalse(documentCaptorWrite.docId().contains("urn:li:"));
  }

  @Test
  public void testProcessBatch_CallsAppendRunIdBySearchGroupWhenRunIdPresent() throws Exception {
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-456");
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

    strategy.processBatch(operationContext, groupedEvents, true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    assertEquals(
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS).docId(),
        expectedDocId);
    verify(elasticSearchService)
        .appendRunIdBySearchGroup(
            eq(operationContext), eq("dataset"), eq(expectedDocId), eq(testUrn), eq("run-456"));
  }

  @Test
  public void testProcessBatch_ReplacementHasherChangesDocId() throws Exception {
    EntityDocumentIdHasher replacement =
        (operation, urn) -> DigestUtils.sha256Hex("extra|" + urn.toString());
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .entityDocumentIdHasher(replacement)
            .build();
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);

    String defaultId = new Sha256UrnEntityDocumentIdHasher().documentId(operationContext, testUrn);
    String replacedId = replacement.documentId(operationContext, testUrn);
    V3SearchDocumentWrite docIdCaptorWrite = v3Write(operationContext, null, DOCUMENT_KINDS);
    assertEquals(docIdCaptorWrite.docId(), replacedId);
    assertFalse(docIdCaptorWrite.docId().equals(defaultId));
  }

  @Test
  public void testProcessBatch_DocumentContributorAddsRootField() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("_ext", "1");
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .documentContributors(List.of(contributor))
            .build();
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);
    V3SearchDocumentWrite documentCaptorWrite = v3Write(operationContext, null, DOCUMENT_KINDS);
    assertTrue(documentCaptorWrite.source().contains("\"_ext\":\"1\""));
  }

  @Test
  public void testProcessBatch_DocumentContributorCannotOverwriteUrn() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("urn", "overwritten");
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .documentContributors(List.of(contributor))
            .build();
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    expectThrows(
        IllegalStateException.class,
        () ->
            strategy.processBatch(
                operationContext,
                Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
                true));
    verify(elasticSearchService, never()).writeV3SearchDocument(any(), any());
  }

  @Test
  public void testProcessBatch_DocumentContributorOverwriteDoesNotFailDualWrite() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("urn", "overwritten");
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .documentContributors(List.of(contributor))
            .v2Enabled(true)
            .build();
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testProcessBatch_DocumentContributorCannotOverwriteRunId() throws Exception {
    V3SearchDocumentContributor contributor =
        (operation, urn, document) -> document.put("runId", "stolen");
    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .documentContributors(List.of(contributor))
            .build();
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));

    expectThrows(
        IllegalStateException.class,
        () ->
            strategy.processBatch(
                operationContext,
                Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
                true));
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
    v3Write(operationContext, null, DOCUMENT_KINDS); // doc id
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
    v3Write(operationContext, null, DOCUMENT_KINDS); // doc id
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
  public void testGetIndexMappingsWithNewStructuredProperty() throws IOException {
    StructuredPropertyDefinition property =
        new StructuredPropertyDefinition()
            .setQualifiedName("retention")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"))
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")));
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:retention");

    Collection<MappingsBuilder.IndexMapping> mappings =
        strategy.getIndexMappingsWithNewStructuredProperty(operationContext, propertyUrn, property);

    // As on V2, the strategy returns its mappings builder's mappings for the new property
    assertFalse(mappings.isEmpty());
    assertEquals(
        mappings,
        new MultiEntityMappingsBuilder(
                EntityIndexConfiguration.builder().v3(v3Config).build(),
                null,
                ESUtils.KEYWORD_MAXLENGTH,
                List.of())
            .getIndexMappingsWithNewStructuredProperty(operationContext, propertyUrn, property));
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
                UpdateIndicesV3Strategy.builder()
                    .v3Config(v3Config)
                    .elasticSearchService(elasticSearchService)
                    .searchDocumentTransformer(searchDocumentTransformer)
                    .timeseriesAspectService(timeseriesAspectService)
                    .build());

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
                UpdateIndicesV3Strategy.builder()
                    .v3Config(v3Config)
                    .elasticSearchService(elasticSearchService)
                    .searchDocumentTransformer(searchDocumentTransformer)
                    .timeseriesAspectService(timeseriesAspectService)
                    .build());

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
                UpdateIndicesV3Strategy.builder()
                    .v3Config(v3Config)
                    .elasticSearchService(elasticSearchService)
                    .searchDocumentTransformer(searchDocumentTransformer)
                    .timeseriesAspectService(timeseriesAspectService)
                    .build());

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
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .build();

    // Verify strategy was created successfully
    assertTrue(strategyWithNullConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsEmpty() {
    // Setup v3Config with empty mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithEmptyConfig =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .build();

    // Verify strategy was created successfully
    assertTrue(strategyWithEmptyConfig.isEnabled());
  }

  @Test
  public void testConstructor_SuccessWhenMappingConfigIsWhitespace() {
    // Setup v3Config with whitespace-only mapping configuration (should not throw exception)
    when(v3Config.getMappingConfig()).thenReturn("   ");

    // Execute - should not throw exception
    UpdateIndicesV3Strategy strategyWithWhitespaceConfig =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .build();

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
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), anyBoolean());

    // Execute - the method catches the RuntimeException and logs it, so no exception is thrown
    strategy.updateIndexMappings(
        operationContext, testUrn, mockEntitySpec, mockAspectSpec, mockAspect, null);

    // Verify that applyMappings was called (which would have thrown the IOException)
    verify(mockIndexBuilder)
        .applyMappings(any(OperationContext.class), any(ReindexConfig.class), eq(false));
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
      assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
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

      v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND);
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

      strategy.processBatch(operationContext, groupedEvents, true);

      // Unset searchGroup uses the entity type as the V3 index key
      v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND);
    }
  }

  @Test
  public void testProcessBatch_EmptyEventsListForUrn() throws Exception {
    // Setup for URN with empty events list (different from empty groupedEvents map)
    Map<Urn, List<MCLItem>> groupedEvents =
        Collections.singletonMap(testUrn, Collections.emptyList());

    strategy.processBatch(operationContext, groupedEvents, true);

    verify(elasticSearchService, never()).writeV3SearchDocument(any(), any());
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

    v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
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
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
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
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  // ==================== Timeseries throttle tests ====================

  private TimeseriesWriteThrottleCache buildThrottleCache(
      boolean entityEnabled, boolean tsEnabled, boolean observeEnabled) {
    return new TimeseriesWriteThrottleCache(
        com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration.builder()
            .entityIndex(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(entityEnabled)
                    .build())
            .timeseriesIndex(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(tsEnabled)
                    .build())
            .observe(
                com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                    .IndexThrottleConfig.builder()
                    .enabled(observeEnabled)
                    .build())
            .refreshPeriodSeconds(3600)
            .maxCacheUrns(10_000)
            .build());
  }

  @Test
  public void testThrottle_EntityIndexSuppressesTimeseriesAspect() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, false);
    UpdateIndicesV3Strategy throttledStrategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .timeseriesThrottleCache(cache)
            .build();

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);

    // Prime cache
    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

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

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Timeseries aspect should be excluded from the document (throttled)
    // Since it's the only aspect, no upsert should happen
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testThrottle_ObserveModeDoesNotSuppressInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(false, false, true);
    UpdateIndicesV3Strategy throttledStrategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .timeseriesThrottleCache(cache)
            .build();

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);

    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

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

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Observe mode should NOT suppress — the write should proceed
    v3Write(operationContext, null, DOCUMENT_KINDS);
  }

  @Test
  public void testThrottle_FirstWritePassesThroughInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, false);
    UpdateIndicesV3Strategy throttledStrategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .timeseriesThrottleCache(cache)
            .build();

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_000_000L);
    // No prior cache entry → not throttled

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

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // First write should not be throttled
    v3Write(operationContext, null, DOCUMENT_KINDS);
  }

  @Test
  public void testThrottle_EntityAndObserveBothEnabledInV3() throws Exception {
    TimeseriesWriteThrottleCache cache = buildThrottleCache(true, false, true);
    UpdateIndicesV3Strategy throttledStrategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .timeseriesThrottleCache(cache)
            .build();

    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_001_000L);

    cache.recordWrite(testUrn.toString(), "datasetProfile", 1_000_000_000L);

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

    throttledStrategy.processBatch(operationContext, groupedEvents, true);

    // Entity index enabled suppresses the write, observe mode logs but doesn't write
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testThrottle_NullCacheNoThrottling() throws Exception {
    // Default strategy (null cache) should not throttle timeseries aspects
    when(mockAspectSpec.getName()).thenReturn("datasetProfile");
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockEvent.getAspectName()).thenReturn("datasetProfile");
    when(mockAuditStamp.getTime()).thenReturn(1_000_000_000L);

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

    strategy.processBatch(operationContext, groupedEvents, true);

    // With null throttle cache, all writes proceed
    v3Write(operationContext, null, DOCUMENT_KINDS);
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
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testLiftsEmbeddingsToDocumentV3RootWhenSemanticEnabled() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn("document");
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", documentUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);
    semanticDoc.put("skipReason", "EMPTY_TEXT");

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(Set.of("document"));
    ModelEmbeddingConfig model = new ModelEmbeddingConfig();
    model.setVectorDimension(1024);
    semanticConfig.setModels(Map.of("cohere_embed_v3", model));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);
    V3SearchDocumentWrite documentCaptorWrite =
        v3Write(operationContext, groupIndex(operationContext, "document"), DOCUMENT_KINDS);
    ObjectNode written =
        (ObjectNode)
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(documentCaptorWrite.source());
    assertTrue(written.has("embeddings"));
    assertEquals(written.get("skipReason").asText(), "EMPTY_TEXT");
    assertFalse(written.has("resolvedTextSha256"));
    assertFalse(written.get("_aspects").get("semanticContent").has("embeddings"));
    assertFalse(written.get("_aspects").get("semanticContent").has("skipReason"));
  }

  @Test
  public void testDoesNotLiftEmbeddingsWhenSemanticDisabled() throws Exception {
    Urn documentUrn = UrnUtils.getUrn("urn:li:document:bridge-1");
    when(mockEvent.getUrn()).thenReturn(documentUrn);
    when(mockEntitySpec.getName()).thenReturn("document");
    when(mockEntitySpec.getSearchGroup()).thenReturn("document");
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", documentUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(false);
    semanticConfig.setEnabledEntities(Set.of("document"));
    semanticConfig.setModels(Map.of("cohere_embed_v3", new ModelEmbeddingConfig()));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(documentUrn, Collections.singletonList(mockEvent)),
        true);
    V3SearchDocumentWrite documentCaptorWrite =
        v3Write(operationContext, groupIndex(operationContext, "document"), DOCUMENT_KINDS);
    ObjectNode written =
        (ObjectNode)
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(documentCaptorWrite.source());
    assertFalse(written.has("embeddings"));
    assertTrue(written.get("_aspects").get("semanticContent").has("embeddings"));
  }

  @Test
  public void testDoesNotLiftEmbeddingsForDataset() throws Exception {
    when(mockAspectSpec.getName()).thenReturn("semanticContent");
    when(mockEvent.getAspectName()).thenReturn("semanticContent");

    ObjectNode semanticDoc = JsonNodeFactory.instance.objectNode();
    semanticDoc.put("urn", testUrn.toString());
    ObjectNode embeddings = JsonNodeFactory.instance.objectNode();
    embeddings.put("model", "cohere_embed_v3");
    semanticDoc.set("embeddings", embeddings);

    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(semanticDoc));

    SemanticSearchConfiguration semanticConfig = new SemanticSearchConfiguration();
    semanticConfig.setEnabled(true);
    semanticConfig.setEnabledEntities(Set.of("document"));
    semanticConfig.setModels(Map.of("cohere_embed_v3", new ModelEmbeddingConfig()));

    strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .semanticSearchConfiguration(semanticConfig)
            .build();

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, Collections.singletonList(mockEvent)),
        true);
    V3SearchDocumentWrite documentCaptorWrite =
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
    ObjectNode written =
        (ObjectNode)
            new com.fasterxml.jackson.databind.ObjectMapper()
                .readTree(documentCaptorWrite.source());
    assertFalse(written.has("embeddings"));
  }

  @Test
  public void testProcessBatch_ProjectsAspectFieldsToRootAndAspects() throws Exception {
    RecordTemplate previousAspect = mock(RecordTemplate.class);
    when(mockEvent.getAspectName()).thenReturn(DATASET_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(previousAspect);
    when(mockEvent.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("run-1"));
    stubTransform(mockAspect, Map.of("name", "orders"));
    stubTransform(previousAspect, Map.of("name", "old", "description", "dropped"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), false);

    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "orders");
    // A partial update keeps whatever it does not overwrite, so removed values are nulled
    assertTrue(written.get("description").isNull());
    assertFalse(written.has("_systemMetadata"));
    JsonNode aspect = written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(aspect.get("name").asText(), "orders");
    assertTrue(aspect.get("description").isNull());
    assertEquals(aspect.get("_systemMetadata").get("runId").asText(), "run-1");
  }

  @Test
  public void testProcessBatch_EditableAspectWinsSharedRootField() throws Exception {
    MCLItem editableEvent = aspectEvent("editableDatasetProperties");
    MCLItem baseEvent = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate editableRecord = editableEvent.getRecordTemplate();
    RecordTemplate baseRecord = baseEvent.getRecordTemplate();
    stubTransform(editableRecord, Map.of("displayName", "edited"));
    stubTransform(baseRecord, Map.of("displayName", "ingested"));

    // The base aspect arrives last, but the user-edited override still wins the root field
    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, List.of(editableEvent, baseEvent)),
        false);

    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("displayName").asText(), "edited");
    assertEquals(
        written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("displayName").asText(),
        "ingested");
  }

  @Test
  public void testProcessBatch_CoalescesAspectEventsAgainstBatchBaseline() throws Exception {
    RecordTemplate before = mock(RecordTemplate.class);
    MCLItem first = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem second = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate firstRecord = first.getRecordTemplate();
    RecordTemplate secondRecord = second.getRecordTemplate();
    when(first.getPreviousRecordTemplate()).thenReturn(before);
    when(second.getPreviousRecordTemplate()).thenReturn(firstRecord);
    stubTransform(before, Map.of("name", "a", "description", "kept by the first event"));
    stubTransform(secondRecord, Map.of("name", "c"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(first, second)), false);

    // Diffed against the value before the batch, so the removal reaches _aspects too
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "c");
    assertTrue(written.get("description").isNull());
    JsonNode aspect = written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(aspect.get("name").asText(), "c");
    assertTrue(aspect.get("description").isNull());
  }

  @Test
  public void testProcessBatch_RemovedOverrideKeepsBaseRootValue() throws Exception {
    MCLItem baseEvent = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem editableEvent = aspectEvent("editableDatasetProperties");
    RecordTemplate baseRecord = baseEvent.getRecordTemplate();
    RecordTemplate editableRecord = editableEvent.getRecordTemplate();
    RecordTemplate previousOverride = mock(RecordTemplate.class);
    when(editableEvent.getPreviousRecordTemplate()).thenReturn(previousOverride);
    stubTransform(baseRecord, Map.of("displayName", "ingested"));
    stubTransform(editableRecord, Map.of());
    stubTransform(previousOverride, Map.of("displayName", "edited"));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, List.of(baseEvent, editableEvent)),
        false);

    // The override was removed, so the root falls back to the ingested value
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("displayName").asText(), "ingested");
    assertTrue(
        written.get("_aspects").get("editableDatasetProperties").get("displayName").isNull());
  }

  @Test
  public void testProcessBatch_RemovedOverrideFallsBackToUnchangedBaseAspect() throws Exception {
    MCLItem baseEvent = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem editableEvent = aspectEvent("editableDatasetProperties");
    Status baseRecord = new Status().setRemoved(false);
    Status basePrevious = new Status().setRemoved(false);
    when(baseEvent.getRecordTemplate()).thenReturn(baseRecord);
    when(baseEvent.getPreviousRecordTemplate()).thenReturn(basePrevious);
    RecordTemplate editableRecord = editableEvent.getRecordTemplate();
    RecordTemplate previousOverride = mock(RecordTemplate.class);
    when(editableEvent.getPreviousRecordTemplate()).thenReturn(previousOverride);
    stubTransform(baseRecord, Map.of("displayName", "ingested"));
    stubTransform(editableRecord, Map.of());
    stubTransform(previousOverride, Map.of("displayName", "edited"));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, List.of(baseEvent, editableEvent)),
        false);

    // The unchanged base aspect is not rewritten, but the root still falls back to its value
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("displayName").asText(), "ingested");
    assertFalse(written.get("_aspects").has(DATASET_PROPERTIES_ASPECT_NAME));
    assertTrue(
        written.get("_aspects").get("editableDatasetProperties").get("displayName").isNull());
  }

  @Test
  public void testProcessBatch_UnchangedOverrideKeepsSharedRootField() throws Exception {
    MCLItem baseEvent = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem editableEvent = aspectEvent("editableDatasetProperties");
    Status baseRecord = new Status().setRemoved(true);
    when(baseEvent.getRecordTemplate()).thenReturn(baseRecord);
    Status editableRecord = new Status().setRemoved(false);
    Status editablePrevious = new Status().setRemoved(false);
    when(editableEvent.getRecordTemplate()).thenReturn(editableRecord);
    when(editableEvent.getPreviousRecordTemplate()).thenReturn(editablePrevious);
    stubTransform(baseRecord, Map.of("displayName", "ingested"));
    stubTransform(editableRecord, Map.of("displayName", "edited", "editedDescription", "kept"));

    strategy.processBatch(
        operationContext,
        Collections.singletonMap(testUrn, List.of(baseEvent, editableEvent)),
        false);

    // The unchanged override still wins the root field its base aspect rewrote; nothing else of
    // it is rewritten
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("displayName").asText(), "edited");
    assertFalse(written.has("editedDescription"));
    assertFalse(written.get("_aspects").has("editableDatasetProperties"));
  }

  @Test
  public void testProcessBatch_RestateThenUpdateNullsDroppedField() throws Exception {
    MCLItem restate = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem update = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate restated = restate.getRecordTemplate();
    RecordTemplate updated = update.getRecordTemplate();
    when(restate.getChangeType()).thenReturn(ChangeType.RESTATE);
    when(update.getPreviousRecordTemplate()).thenReturn(restated);
    stubTransform(restated, Map.of("name", "a", "description", "dropped by the update"));
    stubTransform(updated, Map.of("name", "b"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(restate, update)), false);

    // The restate has no previous value, so the update's previous value is the baseline
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "b");
    assertTrue(written.get("description").isNull());
  }

  @Test
  public void testProcessBatch_CreateThenUpdateNullsNoFieldTheIndexNeverHeld() throws Exception {
    MCLItem create = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem update = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate created = create.getRecordTemplate();
    RecordTemplate updated = update.getRecordTemplate();
    when(update.getPreviousRecordTemplate()).thenReturn(created);
    stubTransform(created, Map.of("name", "a", "description", "dropped by the update"));
    stubTransform(updated, Map.of("name", "b"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(create, update)), false);

    // The index held no value of the aspect, so a null here could only clear a root field that
    // another aspect supplies
    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertEquals(written.get("name").asText(), "b");
    assertFalse(written.has("description"));
  }

  @Test
  public void testProcessBatch_WritesCreatedAspectFollowedByNoOp() throws Exception {
    MCLItem create = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem noOp = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status created = new Status().setRemoved(false);
    Status unchanged = new Status().setRemoved(false);
    when(create.getRecordTemplate()).thenReturn(created);
    when(noOp.getRecordTemplate()).thenReturn(unchanged);
    when(noOp.getPreviousRecordTemplate()).thenReturn(created);
    stubTransform(unchanged, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(create, noOp)), false);

    // The index did not hold the aspect before the batch, so it is written
    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  @Test
  public void testProcessBatch_WritesAspectForcedInsideBatch() throws Exception {
    Status value = new Status().setRemoved(false);
    List<MCLItem> batch = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
      when(event.getRecordTemplate()).thenReturn(value);
      when(event.getPreviousRecordTemplate()).thenReturn(value);
      batch.add(event);
    }
    when(batch.get(1).getSystemMetadata())
        .thenReturn(
            new SystemMetadata().setProperties(new StringMap(Map.of(FORCE_INDEXING_KEY, "true"))));
    stubTransform(value, Map.of("removed", "false"));

    strategy.processBatch(operationContext, Collections.singletonMap(testUrn, batch), false);

    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  @Test
  public void testProcessBatch_SkipsDeleteThenRecreateWithSameValue() throws Exception {
    MCLItem delete = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem recreate = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status value = new Status().setRemoved(false);
    when(delete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(delete.getPreviousRecordTemplate()).thenReturn(value);
    when(recreate.getRecordTemplate()).thenReturn(new Status().setRemoved(false));
    stubTransform(value, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(delete, recreate)), false);

    // The index ends the batch holding what it held before it
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testProcessBatch_UpdateThenDeleteNullsFieldsHeldBeforeBatch() throws Exception {
    MCLItem update = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem delete = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    RecordTemplate before = mock(RecordTemplate.class);
    RecordTemplate updated = update.getRecordTemplate();
    when(update.getPreviousRecordTemplate()).thenReturn(before);
    when(delete.getChangeType()).thenReturn(ChangeType.DELETE);
    when(delete.getPreviousRecordTemplate()).thenReturn(updated);
    RecordTemplate deleted = delete.getRecordTemplate();
    stubTransform(before, Map.of("name", "a"));
    stubTransform(deleted, Map.of());

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(update, delete)), false);

    ObjectNode written = capturedDocument(DATASET_ENTITY_NAME);
    assertTrue(written.get("name").isNull());
    assertTrue(written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("name").isNull());
  }

  @Test
  public void testProcessBatch_AppliesAspectsInFirstAppearanceOrder() throws Exception {
    MCLItem first = aspectEvent("dataPlatformInstance");
    MCLItem other = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    MCLItem last = aspectEvent("dataPlatformInstance");
    RecordTemplate otherRecord = other.getRecordTemplate();
    RecordTemplate lastRecord = last.getRecordTemplate();
    stubTransform(otherRecord, Map.of("platform", "from datasetProperties"));
    stubTransform(lastRecord, Map.of("platform", "from dataPlatformInstance"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(first, other, last)), false);

    // As on V2, an aspect is written at its first position with its last value
    assertEquals(
        capturedDocument(DATASET_ENTITY_NAME).get("platform").asText(), "from datasetProperties");
  }

  @Test
  public void testProcessBatch_SkipsUnchangedAspect() throws Exception {
    MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status status = new Status().setRemoved(false);
    when(event.getRecordTemplate()).thenReturn(status);
    when(event.getPreviousRecordTemplate()).thenReturn(new Status().setRemoved(false));
    stubTransform(status, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(event)), false);

    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testProcessBatch_UnchangedBatchStillRunsMappingHook() throws Exception {
    MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status status = new Status().setRemoved(false);
    when(event.getRecordTemplate()).thenReturn(status);
    when(event.getPreviousRecordTemplate()).thenReturn(new Status().setRemoved(false));
    stubTransform(status, Map.of("removed", "false"));
    UpdateIndicesV3Strategy spiedStrategy = spy(strategy);

    spiedStrategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(event)), true);

    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
    // Re-saving an unchanged property definition is how a failed mapping update is repaired
    verify(spiedStrategy)
        .updateIndexMappings(
            eq(operationContext),
            eq(testUrn),
            eq(mockEntitySpec),
            any(AspectSpec.class),
            eq(status),
            any());
  }

  @Test
  public void testProcessBatch_ForceIndexingWritesUnchangedAspect() throws Exception {
    MCLItem event = aspectEvent(DATASET_PROPERTIES_ASPECT_NAME);
    Status status = new Status().setRemoved(false);
    SystemMetadata systemMetadata =
        new SystemMetadata().setProperties(new StringMap(Map.of(FORCE_INDEXING_KEY, "true")));
    when(event.getRecordTemplate()).thenReturn(status);
    when(event.getPreviousRecordTemplate()).thenReturn(new Status().setRemoved(false));
    when(event.getSystemMetadata()).thenReturn(systemMetadata);
    stubTransform(status, Map.of("removed", "false"));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(event)), false);

    assertEquals(capturedDocument(DATASET_ENTITY_NAME).get("removed").asText(), "false");
  }

  private MCLItem aspectEvent(String aspectName) {
    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);
    MCLItem event = mock(MCLItem.class);
    when(event.getUrn()).thenReturn(testUrn);
    when(event.getEntitySpec()).thenReturn(mockEntitySpec);
    when(event.getAspectSpec()).thenReturn(aspectSpec);
    when(event.getAspectName()).thenReturn(aspectName);
    when(event.getRecordTemplate()).thenReturn(mock(RecordTemplate.class));
    when(event.getAuditStamp()).thenReturn(mockAuditStamp);
    when(event.getChangeType()).thenReturn(ChangeType.UPSERT);
    return event;
  }

  private void stubTransform(RecordTemplate record, Map<String, String> fields) throws Exception {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put("urn", testUrn.toString());
    fields.forEach(document::put);
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            eq(record),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenReturn(Optional.of(document));
  }

  private ObjectNode capturedDocument(String searchGroup) throws Exception {
    V3SearchDocumentWrite write =
        v3Write(operationContext, groupIndex(operationContext, searchGroup), DOCUMENT_KINDS);
    return (ObjectNode) new com.fasterxml.jackson.databind.ObjectMapper().readTree(write.source());
  }

  private static final Set<V3SearchDocumentWrite.Kind> DOCUMENT_KINDS =
      Set.of(
          V3SearchDocumentWrite.Kind.MERGE,
          V3SearchDocumentWrite.Kind.MERGE_EXISTING,
          V3SearchDocumentWrite.Kind.CREATE,
          V3SearchDocumentWrite.Kind.REPLACE);
  private static final Set<V3SearchDocumentWrite.Kind> TOMBSTONE_KIND =
      Set.of(V3SearchDocumentWrite.Kind.TOMBSTONE);
  private static final Set<V3SearchDocumentWrite.Kind> DELETE_KIND =
      Set.of(V3SearchDocumentWrite.Kind.DELETE);

  /**
   * The V3 writes issued under {@code context} to {@code indexName} with one of {@code kinds}, in
   * order. A null context or index matches any.
   */
  private List<V3SearchDocumentWrite> v3Writes(
      OperationContext context, String indexName, Set<V3SearchDocumentWrite.Kind> kinds) {
    ArgumentCaptor<V3SearchDocumentWrite> captor =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(elasticSearchService, atLeast(0))
        .writeV3SearchDocument(context == null ? any() : eq(context), captor.capture());
    return captor.getAllValues().stream()
        .filter(write -> indexName == null || write.indexName().equals(indexName))
        .filter(write -> kinds.contains(write.kind()))
        .toList();
  }

  /**
   * The one V3 write issued under {@code context} to {@code indexName} with one of {@code kinds}.
   */
  private V3SearchDocumentWrite v3Write(
      OperationContext context, String indexName, Set<V3SearchDocumentWrite.Kind> kinds) {
    List<V3SearchDocumentWrite> writes = v3Writes(context, indexName, kinds);
    assertEquals(writes.size(), 1, "V3 writes to " + indexName + " of " + kinds + ": " + writes);
    return writes.get(0);
  }

  private static String groupIndex(OperationContext context, String searchGroup) {
    return context
        .getSearchContext()
        .getIndexConvention()
        .getEntityIndexNameV3(context, searchGroup);
  }

  private static final String FILL_TARGET = "datasetindex_v3_0_14_0-0_1679000000000";

  /** A change log carrying the headers a backfill stamps; {@code target} may be null. */
  private static MetadataChangeLog backfillChangeLog(String mode, String target) {
    Map<String, String> headers = new HashMap<>();
    headers.put(BackfillHeaders.MODE_HEADER_NAME, mode);
    if (BackfillHeaders.MODE_BASELINE.equals(mode)) {
      headers.put(BackfillHeaders.COMPLETE_ENTITY_HEADER_NAME, Boolean.TRUE.toString());
    }
    if (target != null) {
      headers.put(BackfillHeaders.TARGET_INDEX_HEADER_NAME, target);
    }
    return new MetadataChangeLog().setHeaders(new StringMap(headers));
  }

  private void stubTransformedDocument() throws Exception {
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any(AuditStamp.class)))
        .thenReturn(Optional.of(mockSearchDocument));
  }

  @Test
  public void testBaselineFillNamingATargetIndexWritesThereInsteadOfTheAlias() throws Exception {
    stubTransformedDocument();
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-fill");
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_BASELINE, FILL_TARGET));
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    V3SearchDocumentWrite write = v3Write(operationContext, FILL_TARGET, DOCUMENT_KINDS);
    assertEquals(write.docId(), expectedDocId);
    assertEquals(write.kind(), V3SearchDocumentWrite.Kind.CREATE);
    // The create carries the run ids: an update could reach the document before the fill does.
    JsonNode written = operationContext.getObjectMapper().readTree(write.source());
    assertEquals(written.get(MappingConstants.RUN_ID_FIELD).get(0).asText(), "run-fill");
    verify(elasticSearchService, never())
        .appendRunIdByIndexName(any(), anyString(), anyString(), any(Urn.class), anyString());
    assertEquals(v3Writes(null, null, DOCUMENT_KINDS).size(), 1, "nothing reaches the alias");
    // A backfill checkpoints what it has written once the batch returns, so the batch is drained.
    verify(writeDAO).flushAndWait();
  }

  @Test
  public void testBaselineMaintenanceRunReplacesTheAliasDocument() throws Exception {
    stubTransformedDocument();
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_BASELINE, null));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, liveAlias(), DOCUMENT_KINDS).kind(),
        V3SearchDocumentWrite.Kind.REPLACE);
  }

  @Test
  public void testLiveWriteUpsertsWhileTheAliasIsFilledInPlace() throws Exception {
    stubTransformedDocument();
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);

    strategyWithTargets(targets)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, liveAlias(), DOCUMENT_KINDS).kind(),
        V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testTimeseriesWriteDoesNotCreateADocumentWhileTheAliasIsFilledInPlace()
      throws Exception {
    stubTransformedDocument();
    when(mockAspectSpec.isTimeseries()).thenReturn(true);
    when(mockSystemMetadata.hasRunId()).thenReturn(true);
    when(mockSystemMetadata.getRunId()).thenReturn("run-ts");
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    when(targets.activeFillTargetFor(liveAlias())).thenReturn(Optional.of(liveAlias()));

    strategyWithTargets(targets)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, liveAlias(), DOCUMENT_KINDS).kind(),
        V3SearchDocumentWrite.Kind.MERGE_EXISTING);
    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    verify(elasticSearchService, never())
        .appendRunIdBySearchGroup(any(), anyString(), anyString(), any(Urn.class), anyString());
    verify(elasticSearchService)
        .appendRunIdToExistingDocument(
            eq(operationContext), eq(liveAlias()), eq(expectedDocId), eq("run-ts"));
  }

  @Test
  public void testLiveWriteUpsertsOutsideAnInPlaceFill() throws Exception {
    stubTransformedDocument();

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, liveAlias(), DOCUMENT_KINDS).kind(),
        V3SearchDocumentWrite.Kind.MERGE);
  }

  @Test
  public void testSteadyStateLiveWriteDoesNotHydrateEntityAspectsOrTimeseries() throws Exception {
    stubTransformedDocument();

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    verify(timeseriesAspectService, never())
        .getLatestTimeseriesAspectValues(any(), any(), any(), any());
    verify(elasticSearchService, never()).aspectFieldNamesBySearchGroup(any(), anyString(), any());
  }

  @Test
  public void testKeyDeleteTombstonesTheDocumentAndMirrorsTheTombstoneIntoTheFillTarget()
      throws Exception {
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String next = liveAlias() + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(liveAlias())).thenReturn(Optional.of(next));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);

    strategyWithTargets(targets)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    V3SearchDocumentWrite delete = v3Write(operationContext, liveAlias(), DELETE_KIND);
    assertEquals(delete.source(), "");
    V3SearchDocumentWrite tombstone = v3Write(operationContext, next, TOMBSTONE_KIND);
    JsonNode written = operationContext.getObjectMapper().readTree(tombstone.source());
    assertTrue(written.get(V3SearchDocumentProjector.REMOVED_FIELD).asBoolean());
    assertTrue(written.get(V3SearchDocumentProjector.TOMBSTONE_FIELD).asBoolean());
    assertEquals(written.get("urn").asText(), testUrn.toString());
    // The fill may not have created the document yet; the target tombstone keeps it from coming
    // back.
    assertTrue(v3Writes(operationContext, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testKeyDeleteTombstonesTheAliasWhileItIsFilledInPlace() throws Exception {
    // An in-place fill creates into the live alias, so the placeholder has to land there. A delete
    // would let the fill recreate the entity, and nothing replays the hard delete, so the batch
    // waits for the tombstone.
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    when(targets.activeFillTargetFor(liveAlias())).thenReturn(Optional.of(liveAlias()));
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);

    strategyWithTargets(targets)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    V3SearchDocumentWrite tombstone = v3Write(operationContext, liveAlias(), TOMBSTONE_KIND);
    JsonNode written = operationContext.getObjectMapper().readTree(tombstone.source());
    assertTrue(written.get(V3SearchDocumentProjector.REMOVED_FIELD).asBoolean());
    assertTrue(written.get(V3SearchDocumentProjector.TOMBSTONE_FIELD).asBoolean());
    assertTrue(v3Writes(null, null, DELETE_KIND).isEmpty());
    verify(writeDAO).flushAndWait();
  }

  @Test
  public void testKeyRestoreClearsTheHardDeleteMarker() throws Exception {
    stubTransformedDocument();
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEntitySpec.getKeyAspectName()).thenReturn("datasetKey");
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(null);

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = capturedDocument("dataset");
    assertFalse(written.get(V3SearchDocumentProjector.REMOVED_FIELD).asBoolean());
    assertTrue(written.get(V3SearchDocumentProjector.TOMBSTONE_FIELD).isNull());
  }

  @Test
  public void testBaselineFillHydratesTheLatestTimeseriesAspects() throws Exception {
    stubTransformedDocument();
    AspectSpec profileSpec = mock(AspectSpec.class);
    when(profileSpec.getName()).thenReturn("datasetProfile");
    when(profileSpec.isTimeseries()).thenReturn(true);
    when(profileSpec.getSearchableFieldSpecs())
        .thenReturn(List.of(mock(SearchableFieldSpec.class)));
    when(profileSpec.getDataTemplateClass())
        .thenAnswer(invocation -> com.linkedin.dataset.DatasetProfile.class);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    when(mockEntitySpec.getAspectSpec("datasetProfile")).thenReturn(profileSpec);
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_BASELINE, FILL_TARGET));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    com.linkedin.dataset.DatasetProfile profile =
        new com.linkedin.dataset.DatasetProfile().setTimestampMillis(1L).setRowCount(42L);
    when(timeseriesAspectService.getLatestTimeseriesAspectValues(
            any(), eq(Set.of(testUrn)), eq(Set.of("datasetProfile")), any()))
        .thenReturn(
            Map.of(
                testUrn,
                Map.of(
                    "datasetProfile",
                    new com.linkedin.metadata.aspect.EnvelopedAspect()
                        .setAspect(GenericRecordUtils.serializeAspect(profile))
                        .setSystemMetadata(new SystemMetadata()))));

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    ArgumentCaptor<RecordTemplate> projected = ArgumentCaptor.forClass(RecordTemplate.class);
    verify(searchDocumentTransformer, atLeastOnce())
        .transformAspect(any(), any(), projected.capture(), any(), anyBoolean(), any());
    assertTrue(
        projected.getAllValues().stream().anyMatch(record -> profile.data().equals(record.data())),
        "the latest profile is projected into the created document");
  }

  @Test
  public void testBaselineFillFailsWhenTimeseriesHydrationFails() throws Exception {
    stubTransformedDocument();
    AspectSpec profileSpec = mock(AspectSpec.class);
    when(profileSpec.getName()).thenReturn("datasetProfile");
    when(profileSpec.isTimeseries()).thenReturn(true);
    when(profileSpec.getSearchableFieldSpecs())
        .thenReturn(List.of(mock(SearchableFieldSpec.class)));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(List.of(mockAspectSpec, profileSpec));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_BASELINE, FILL_TARGET));
    when(timeseriesAspectService.getLatestTimeseriesAspectValues(any(), any(), any(), any()))
        .thenThrow(new RuntimeException("timeseries unavailable"));

    expectThrows(
        RuntimeException.class,
        () ->
            strategy.processBatch(
                operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true));
    verify(elasticSearchService, never()).writeV3SearchDocument(any(), any());
  }

  private UpdateIndicesV3Strategy strategyWithTargets(IncrementalReindexWriteTargets targets) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
        .documentContributors(List.of())
        .incrementalReindexWriteTargets(targets)
        .build();
  }

  @Test
  public void testLiveBatchIsNotDrainedSynchronously() throws Exception {
    stubTransformedDocument();
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
    verify(writeDAO, never()).flushAndWait();
  }

  @Test
  public void testRollbackCopyRepeatsTheWriteOnTheSwappedOutBackingIndex() throws Exception {
    stubTransformedDocument();
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    when(targets.oldBackingIndexFor(alias)).thenReturn(Optional.of(alias + "_old"));
    UpdateIndicesV3Strategy dualWriting =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .incrementalReindexWriteTargets(targets)
            .build();

    dualWriting.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    V3SearchDocumentWrite liveWrite =
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
    assertEquals(liveWrite.docId(), expectedDocId);
    V3SearchDocumentWrite copyWrite = v3Write(operationContext, alias + "_old", DOCUMENT_KINDS);
    assertEquals(copyWrite.docId(), expectedDocId);
    assertEquals(copyWrite.source(), liveWrite.source(), "the copy is the live document");
    verify(targets).recordDualWriteStarted(alias);
  }

  @Test
  public void testReplayFailureIsNotSkippedWhileV2IsDualWritten() throws Exception {
    stubTransformedDocument();
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    doThrow(new RuntimeException("write failed"))
        .when(elasticSearchService)
        .writeV3SearchDocument(any(), any(V3SearchDocumentWrite.class));
    UpdateIndicesV3Strategy dualWriting =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .entityDocumentIdHasher(new Sha256UrnEntityDocumentIdHasher())
            .documentContributors(List.of())
            .v2Enabled(true)
            .build();
    Map<Urn, List<MCLItem>> batch = Collections.singletonMap(testUrn, List.of(mockEvent));

    // A live write: V2 still has the entity, the next event rebuilds its V3 document.
    dualWriting.processBatch(operationContext, batch, true);

    // A replay: nothing comes back for the URN it drops, so the failure must reach the caller.
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    OperationContext replayContext = contextWithStoredAspects(storedDataset("stored"));
    RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> dualWriting.processBatch(replayContext, batch, true));
    assertEquals(thrown.getMessage(), "write failed");
  }

  @Test
  public void testCatchUpReplayRebuildsTheDocumentFromStoredAspects() throws Exception {
    // The replayed row is a snapshot; the document is rebuilt from what primary storage holds when
    // the event is processed, so a value changed since the read is not overlaid with the stale row.
    stubTransformedDocument();
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    Map<String, SystemAspect> stored = storedDataset("stored");
    OperationContext replayContext = contextWithStoredAspects(stored);

    strategy.processBatch(
        replayContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    ArgumentCaptor<RecordTemplate> projected = ArgumentCaptor.forClass(RecordTemplate.class);
    verify(searchDocumentTransformer, atLeastOnce())
        .transformAspect(any(), any(), projected.capture(), any(), anyBoolean(), any());
    assertTrue(
        projected.getAllValues().stream()
            .anyMatch(
                record ->
                    record
                        .data()
                        .equals(
                            stored.get(DATASET_PROPERTIES_ASPECT_NAME).getRecordTemplate().data())),
        "the stored value is projected");
    assertFalse(projected.getAllValues().contains(mockAspect), "the replayed row value is not");
    assertEquals(
        v3Write(replayContext, groupIndex(replayContext, "dataset"), DOCUMENT_KINDS).docId(),
        DigestUtils.sha256Hex(testUrn.toString()));
    assertTrue(v3Writes(null, null, TOMBSTONE_KIND).isEmpty());
  }

  @Test
  public void testCatchUpReplayDeletesTheDocumentOfAnEntityGoneFromStorage() throws Exception {
    // A row read before a hard delete must not come back as a bare document: without the key
    // aspect in storage the entity is gone, whatever the replayed row says.
    stubTransformedDocument();
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    OperationContext replayContext = contextWithStoredAspects(Map.of());

    strategy.processBatch(
        replayContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(replayContext, groupIndex(replayContext, "dataset"), DELETE_KIND).docId(),
        DigestUtils.sha256Hex(testUrn.toString()));
    assertTrue(v3Writes(null, null, DOCUMENT_KINDS).isEmpty());
  }

  @Test
  public void testCatchUpReplayDropsAnAspectStorageNoLongerHolds() throws Exception {
    // The fill wrote the entity with its tags, the tags were removed while nothing mirrored the
    // removal, and a later write put the entity in the catch-up window. The rebuilt events can
    // only write what storage holds and the document is merged, so the replay nulls what the
    // document holds for the aspect storage lost.
    stubTransformedDocument();
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    OperationContext replayContext = contextWithStoredAspects(storedDataset("stored"));
    String documentId = DigestUtils.sha256Hex(testUrn.toString());
    when(elasticSearchService.aspectFieldNamesBySearchGroup(any(), eq("dataset"), any()))
        .thenReturn(
            Map.of(
                documentId,
                Map.of(
                    DATASET_PROPERTIES_ASPECT_NAME,
                    Set.of("name", "_systemMetadata"),
                    "globalTags",
                    Set.of("tags", "_systemMetadata", "runId"),
                    "structuredProperties",
                    Set.of("structuredProperties.my_property"))));
    bindDatasetAspects(replayContext);

    strategy.processBatch(
        replayContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);
    V3SearchDocumentWrite documentWrite =
        v3Write(replayContext, groupIndex(replayContext, "dataset"), DOCUMENT_KINDS);
    assertEquals(documentWrite.docId(), documentId);
    JsonNode written = replayContext.getObjectMapper().readTree(documentWrite.source());
    assertTrue(
        written.get("_aspects").get("globalTags").isNull(), "the lost aspect's entry is nulled");
    assertTrue(written.get("tags").isNull(), "and the root field it projected");
    assertTrue(
        written.get("structuredProperties.my_property").isNull(),
        "a dotted structured-property key the index holds is cleared");
    assertFalse(
        written.has("structuredProperties") && written.get("structuredProperties").isNull(),
        "the parent structuredProperties key is not nulled");
    assertFalse(
        written.get("_aspects").has("structuredProperties"),
        "no entry under _aspects is invented for them");
    assertFalse(written.has("runId"), "a field the strategy owns is not");
    assertFalse(written.has("_systemMetadata"), "nor the per-aspect system metadata key");
    assertFalse(
        written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).isNull(),
        "an aspect storage still holds is written");
  }

  @Test
  public void testCatchUpReplayKeepsARootFieldATimeseriesAspectHoldsWhenAnAspectIsDropped()
      throws Exception {
    // The index holds rowCount from datasetProfile, a timeseries aspect the replay never rebuilds,
    // and a removed versioned aspect projected the same root field name. Dropping the versioned
    // aspect nulls what only it held, not a field the timeseries aspect still owns.
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(DATASET_PROPERTIES_ASPECT_NAME, Map.of("name", "stored")),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME,
                Set.of("name"),
                "globalTags",
                Set.of("tags", "rowCount"),
                "datasetProfile",
                Set.of("rowCount")));

    assertTrue(written.get("_aspects").get("globalTags").isNull());
    assertTrue(written.get("tags").isNull(), "a field only the dropped aspect held is nulled");
    assertFalse(written.has("rowCount"), "a field the timeseries aspect still holds is kept");
  }

  @Test
  public void testCatchUpReplayNullsAFieldARestatedAspectDropped() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(DATASET_PROPERTIES_ASPECT_NAME, Map.of("name", "orders")),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME, Set.of("name", "description", "_systemMetadata")));

    assertEquals(written.get("name").asText(), "orders");
    assertTrue(written.get("description").isNull());
    JsonNode aspect = written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME);
    assertEquals(aspect.get("name").asText(), "orders");
    assertTrue(aspect.get("description").isNull());
  }

  @Test
  public void testCatchUpReplayNullsADroppedKeyInsideAnObjectField() throws Exception {
    ObjectNode customProperties = JsonNodeFactory.instance.objectNode();
    customProperties.put("kept", "yes");
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(DATASET_PROPERTIES_ASPECT_NAME, Map.of("name", "orders")),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME, objectField("customProperties", customProperties)),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME,
                Set.of(
                    "name",
                    "customProperties",
                    nestedField("customProperties", "kept"),
                    nestedField("customProperties", "dropped"))));

    assertEquals(written.get("name").asText(), "orders");
    assertEquals(written.get("customProperties").get("kept").asText(), "yes");
    assertTrue(written.get("customProperties").get("dropped").isNull());
    JsonNode aspectProperties =
        written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("customProperties");
    assertEquals(aspectProperties.get("kept").asText(), "yes");
    assertTrue(aspectProperties.get("dropped").isNull());
  }

  @Test
  public void testCatchUpReplayLeavesATimeseriesKeyInsideASharedObjectField() throws Exception {
    ObjectNode customProperties = JsonNodeFactory.instance.objectNode();
    customProperties.put("kept", "yes");
    String rowCount = nestedField("customProperties", "rowCount");
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME, objectField("customProperties", customProperties)),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME,
                Set.of("customProperties", rowCount),
                "datasetProfile",
                Set.of("customProperties", rowCount)));

    assertEquals(written.get("customProperties").get("kept").asText(), "yes");
    assertFalse(written.get("customProperties").has("rowCount"));
    assertTrue(
        written
            .get("_aspects")
            .get(DATASET_PROPERTIES_ASPECT_NAME)
            .get("customProperties")
            .get("rowCount")
            .isNull());
    assertFalse(written.get("_aspects").has("datasetProfile"));
  }

  @Test
  public void testCatchUpReplayKeepsARootFieldAnotherRestatedAspectSets() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset(
                "stored", Map.of("editableDatasetProperties", new EditableDatasetProperties())),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME,
                Map.of("description", "from properties"),
                "editableDatasetProperties",
                Map.of("name", "edited")),
            Map.of(DATASET_PROPERTIES_ASPECT_NAME, Set.of("name", "description")));

    assertEquals(written.get("name").asText(), "edited");
    assertTrue(written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("name").isNull());
  }

  @Test
  public void testCatchUpReplayNullsADroppedStructuredPropertyAndKeepsTheRest() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset(
                "stored", Map.of(STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties())),
            Map.of(STRUCTURED_PROPERTIES_ASPECT_NAME, Map.of("structuredProperties.other", "kept")),
            Map.of(
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                Set.of("structuredProperties.my_property", "structuredProperties.other")));

    assertTrue(written.get("structuredProperties.my_property").isNull());
    assertEquals(written.get("structuredProperties.other").asText(), "kept");
    assertFalse(
        written.has("structuredProperties") && written.get("structuredProperties").isNull());
  }

  @Test
  public void testCatchUpReplayNullsEveryStructuredPropertyWhenTheAspectIsGone() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(),
            Map.of(
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                Set.of("structuredProperties.my_property", "structuredProperties.other")));

    assertTrue(written.get("structuredProperties.my_property").isNull());
    assertTrue(written.get("structuredProperties.other").isNull());
    assertFalse(
        written.has("structuredProperties") && written.get("structuredProperties").isNull());
  }

  @Test
  public void testCatchUpReplayNullsAStaleAspectsStructuredPropertiesEntry() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset(
                "stored", Map.of(STRUCTURED_PROPERTIES_ASPECT_NAME, structuredProperties())),
            Map.of(STRUCTURED_PROPERTIES_ASPECT_NAME, Map.of("structuredProperties.other", "kept")),
            Map.of(
                ESSearchDAO.STALE_ASPECTS_STRUCTURED_PROPERTIES,
                Set.of(),
                STRUCTURED_PROPERTIES_ASPECT_NAME,
                Set.of("structuredProperties.other")));

    assertTrue(written.get("_aspects").get(STRUCTURED_PROPERTIES_ASPECT_NAME).isNull());
    assertEquals(written.get("structuredProperties.other").asText(), "kept");
  }

  @Test
  public void testCatchUpReplayLeavesTimeseriesFieldsAndRunId() throws Exception {
    JsonNode written =
        catchUpDocument(
            storedDataset("stored"),
            Map.of(DATASET_PROPERTIES_ASPECT_NAME, Map.of("name", "orders")),
            Map.of(
                DATASET_PROPERTIES_ASPECT_NAME,
                Set.of("name", "runId", "rowCount"),
                "datasetProfile",
                Set.of("rowCount")));

    assertEquals(written.get("name").asText(), "orders");
    assertFalse(written.has("runId"));
    assertFalse(written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).has("runId"));
    assertFalse(written.has("rowCount"));
    assertTrue(
        written.get("_aspects").get(DATASET_PROPERTIES_ASPECT_NAME).get("rowCount").isNull());
    assertFalse(written.get("_aspects").has("datasetProfile"));
  }

  @Test
  public void testLiveDeleteIsMirroredIntoAnInFlightRebuildTarget() throws Exception {
    // A rebuild writes a next index while the alias still serves the old one; catch-up cannot
    // replay a hard delete, so the live delete must reach the next index itself.
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(alias)).thenReturn(Optional.of(next));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy mirroring =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .incrementalReindexWriteTargets(targets)
            .build();

    mirroring.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    assertEquals(
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND).docId(),
        expectedDocId);
    assertEquals(v3Write(operationContext, next, TOMBSTONE_KIND).docId(), expectedDocId);
  }

  @Test
  public void testAMirroredDeleteIsDrainedAndIssuedAgainUntilItLands() throws Exception {
    // Nothing replays a hard delete, so a batch that mirrored one into the index a rebuild is
    // filling waits for its writes to land; a drain that fails has the mirror issued again.
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(alias)).thenReturn(Optional.of(next));
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    doThrow(new RuntimeException("Bulk transfer failed")).doNothing().when(writeDAO).flushAndWait();
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy mirroring =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .incrementalReindexWriteTargets(targets)
            .build();

    mirroring.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    verify(writeDAO, times(2)).flushAndWait();
    assertEquals(v3Writes(operationContext, next, TOMBSTONE_KIND).size(), 2);
    assertEquals(
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND).docId(),
        expectedDocId);
  }

  @Test
  public void testAMirroredDeleteDrainThatRunsOutOfTimeLeavesTheWritesQueued() throws Exception {
    // The bulk processor is shared by every consumer thread and idles only when all of them pause,
    // so a wait that runs out is a busy processor, not a failed write.
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(alias)).thenReturn(Optional.of(next));
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    doThrow(new RuntimeException("Timed out waiting for bulk transfer", new TimeoutException()))
        .when(writeDAO)
        .flushAndWait();
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy mirroring =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .incrementalReindexWriteTargets(targets)
            .build();

    mirroring.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    verify(writeDAO).flushAndWait();
    v3Write(operationContext, next, TOMBSTONE_KIND);
  }

  @Test
  public void testAMirroredDeleteWhoseDrainKeepsFailingFailsTheBatch() throws Exception {
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(alias)).thenReturn(Optional.of(next));
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    doThrow(new RuntimeException("Bulk transfer failed")).when(writeDAO).flushAndWait();
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy mirroring =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .incrementalReindexWriteTargets(targets)
            .build();

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class,
            () ->
                mirroring.processBatch(
                    operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true));

    assertEquals(thrown.getMessage(), "Bulk transfer failed");
    verify(writeDAO, times(3)).flushAndWait();
    assertEquals(v3Writes(operationContext, next, TOMBSTONE_KIND).size(), 3);
  }

  @Test
  public void testADeleteWithoutAFillTargetDoesNotWaitForTheBulk() throws Exception {
    // Outside a rebuild a delete costs what it costs today: appended and committed.
    ESWriteDAO writeDAO = mock(ESWriteDAO.class);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(writeDAO);
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);

    strategy.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND).docId(),
        DigestUtils.sha256Hex(testUrn.toString()));
    verify(writeDAO, never()).flushAndWait();
  }

  @Test
  public void testAFailedMirrorIsSkippedLikeAnyLiveWriteWhileV2IsDualWritten() throws Exception {
    // The consumer records a failed batch and moves on without redelivering it, so a mirror that
    // cannot be handed to the bulk processor is logged and skipped like any other live failure:
    // failing the batch would only drop the other entities' writes with it.
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.activeFillTargetFor(alias)).thenReturn(Optional.of(next));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    doThrow(new IllegalStateException("processor closed"))
        .when(elasticSearchService)
        .writeV3SearchDocument(any(), argThat(write -> write.indexName().equals(next)));
    when(mockAspectSpec.getName()).thenReturn("datasetKey");
    when(mockEvent.getAspectName()).thenReturn("datasetKey");
    when(mockEntitySpec.getAspectSpec("datasetKey")).thenReturn(mockAspectSpec);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy dualWriting =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(true)
            .incrementalReindexWriteTargets(targets)
            .build();

    dualWriting.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    assertEquals(
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DELETE_KIND).docId(),
        DigestUtils.sha256Hex(testUrn.toString()));
    v3Write(null, next, TOMBSTONE_KIND);
  }

  @Test
  public void testLiveAspectDeleteIsMirroredIntoAnInFlightRebuildTarget() throws Exception {
    // A removed aspect has no row to replay either. The partial document that clears its fields
    // on the alias is repeated on the next index only when that document already exists, so the
    // create-only fill is not blocked by a partial one.
    stubTransformedDocument();
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    IncrementalReindexWriteTargets targets = mock(IncrementalReindexWriteTargets.class);
    String alias =
        operationContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndexNameV3(operationContext, "dataset");
    String next = alias + "_0_14_0-0_1679000000000";
    when(targets.fillTargetFor(alias)).thenReturn(Optional.of(next));
    when(mockEvent.getChangeType()).thenReturn(ChangeType.DELETE);
    UpdateIndicesV3Strategy mirroring =
        UpdateIndicesV3Strategy.builder()
            .v3Config(v3Config)
            .elasticSearchService(elasticSearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .incrementalReindexWriteTargets(targets)
            .build();

    mirroring.processBatch(
        operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    String expectedDocId = DigestUtils.sha256Hex(testUrn.toString());
    V3SearchDocumentWrite liveWrite =
        v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
    assertEquals(liveWrite.docId(), expectedDocId);
    V3SearchDocumentWrite mirroredWrite = v3Write(operationContext, next, DOCUMENT_KINDS);
    assertEquals(mirroredWrite.docId(), expectedDocId);
    assertEquals(mirroredWrite.kind(), V3SearchDocumentWrite.Kind.MERGE_EXISTING);
    assertEquals(mirroredWrite.source(), liveWrite.source());
  }

  @Test
  public void testRestateWithoutPreviousNullsMappedStructuredPropertiesTheAspectOmits()
      throws Exception {
    StructuredPropertyMappingCache cache = catalogReturning(liveAlias());
    mockSearchDocument.put("name", "table");
    mockSearchDocument.put("structuredProperties.kept", "yes");
    stubRestate();

    strategyWithCatalog(cache)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = writtenDocument(operationContext, "dataset", false);
    assertEquals(written.get("structuredProperties.kept").asText(), "yes");
    assertTrue(written.get("structuredProperties.removed").isNull());
    assertTrue(written.get("structuredProperties.removed__urn_name").isNull());
    assertEquals(written.get("name").asText(), "table");
    assertFalse(written.has("structuredProperties"));
    verify(cache).fields(any(), eq(liveAlias()));
  }

  @Test
  public void testBaselineFillCreatesTheDocumentWithoutNullingFromTheMapping() throws Exception {
    // A created document has nothing to clear, so the fill never reads the mapping catalog.
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(true);
    mockSearchDocument.put("structuredProperties.kept", "yes");
    stubRestate();
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_BASELINE, FILL_TARGET));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));

    strategyWithCatalog(cache)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    V3SearchDocumentWrite write = v3Write(operationContext, FILL_TARGET, DOCUMENT_KINDS);
    assertEquals(write.kind(), V3SearchDocumentWrite.Kind.CREATE);
    JsonNode written = operationContext.getObjectMapper().readTree(write.source());
    assertEquals(written.get("structuredProperties.kept").asText(), "yes");
    assertFalse(written.has("structuredProperties.removed__urn_name"));
    verify(cache, never()).fields(any(), anyString());
  }

  @Test
  public void testUpsertWithPreviousDoesNotNullFromTheMappingCatalog() throws Exception {
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(true);
    mockSearchDocument.put("structuredProperties.kept", "yes");
    stubTransformedDocument();
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getAspectName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.UPSERT);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(mock(RecordTemplate.class));

    strategyWithCatalog(cache)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = writtenDocument(operationContext, "dataset", false);
    assertEquals(written.get("structuredProperties.kept").asText(), "yes");
    assertFalse(written.has("structuredProperties.removed"));
    verify(cache, never()).fields(any(), anyString());
  }

  @Test
  public void testCatchUpReplayDoesNotNullFromTheMappingCatalog() throws Exception {
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(true);
    stubTransformByAspect(Map.of(DATASET_PROPERTIES_ASPECT_NAME, Map.of("name", "stored")));
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    OperationContext replayContext = contextWithStoredAspects(storedDataset("stored"));
    bindDatasetAspects(replayContext);
    when(elasticSearchService.aspectFieldNamesBySearchGroup(any(), eq("dataset"), any()))
        .thenReturn(Map.of());

    strategyWithCatalog(cache)
        .processBatch(replayContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = writtenDocument(replayContext, "dataset", false);
    assertFalse(written.has("structuredProperties.removed"));
    verify(cache, never()).fields(any(), anyString());
  }

  @Test
  public void testRestateWithTheFlagOffDoesNotNullFromTheMappingCatalog() throws Exception {
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(false);
    mockSearchDocument.put("structuredProperties.kept", "yes");
    stubRestate();

    strategyWithCatalog(cache)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = writtenDocument(operationContext, "dataset", false);
    assertEquals(written.get("structuredProperties.kept").asText(), "yes");
    assertFalse(written.has("structuredProperties.removed"));
    verify(cache, never()).fields(any(), anyString());
  }

  @Test
  public void testMappingReadFailureLeavesTheRestateWriteUnchanged() throws Exception {
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(true);
    when(cache.fields(any(), anyString())).thenReturn(Optional.empty());
    mockSearchDocument.put("structuredProperties.kept", "yes");
    stubRestate();

    strategyWithCatalog(cache)
        .processBatch(
            operationContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);

    JsonNode written = writtenDocument(operationContext, "dataset", false);
    assertEquals(written.get("structuredProperties.kept").asText(), "yes");
    assertFalse(written.has("structuredProperties.removed"));
    v3Write(operationContext, groupIndex(operationContext, "dataset"), DOCUMENT_KINDS);
  }

  private StructuredPropertyMappingCache catalogReturning(String indexName) {
    StructuredPropertyMappingCache cache = mock(StructuredPropertyMappingCache.class);
    when(cache.isEnabled()).thenReturn(true);
    when(cache.fields(any(), eq(indexName)))
        .thenReturn(
            Optional.of(
                Set.of(
                    "structuredProperties.kept",
                    "structuredProperties.removed",
                    "structuredProperties.removed__urn_name")));
    return cache;
  }

  private void stubRestate() throws Exception {
    stubTransformedDocument();
    when(mockAspectSpec.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getAspectName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockEvent.getChangeType()).thenReturn(ChangeType.RESTATE);
    when(mockEvent.getPreviousRecordTemplate()).thenReturn(null);
  }

  private UpdateIndicesV3Strategy strategyWithCatalog(StructuredPropertyMappingCache cache) {
    return UpdateIndicesV3Strategy.builder()
        .v3Config(v3Config)
        .elasticSearchService(elasticSearchService)
        .searchDocumentTransformer(searchDocumentTransformer)
        .timeseriesAspectService(timeseriesAspectService)
        .structuredPropertyMappingCache(cache)
        .build();
  }

  private String liveAlias() {
    return operationContext
        .getSearchContext()
        .getIndexConvention()
        .getEntityIndexNameV3(operationContext, "dataset");
  }

  private JsonNode writtenDocument(OperationContext context, String index, boolean byIndexName)
      throws Exception {
    V3SearchDocumentWrite write =
        v3Write(context, byIndexName ? index : groupIndex(context, index), DOCUMENT_KINDS);
    return context.getObjectMapper().readTree(write.source());
  }

  /** A context whose primary storage holds {@code storedAspects} for the test entity. */
  private OperationContext contextWithStoredAspects(Map<String, SystemAspect> storedAspects) {
    return TestOperationContexts.systemContextNoSearchAuthorization(
        storedAspectsRetriever(storedAspects));
  }

  private AspectRetriever storedAspectsRetriever(Map<String, SystemAspect> storedAspects) {
    AspectRetriever retriever = mock(AspectRetriever.class);
    when(retriever.getEntityRegistry()).thenReturn(operationContext.getEntityRegistry());
    when(retriever.getLatestSystemAspects(any(), any()))
        .thenReturn(storedAspects.isEmpty() ? Map.of() : Map.of(testUrn, storedAspects));
    return retriever;
  }

  private void bindDatasetAspects(OperationContext replayContext) {
    EntitySpec dataset = replayContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(dataset.getAspectSpecs());
    when(mockEntitySpec.getKeyAspectName()).thenReturn(dataset.getKeyAspectName());
    when(mockEntitySpec.hasAspect(anyString()))
        .thenAnswer(invocation -> dataset.hasAspect(invocation.getArgument(0)));
    when(mockEntitySpec.getAspectSpec(anyString()))
        .thenAnswer(invocation -> dataset.getAspectSpec(invocation.getArgument(0)));
  }

  private void stubTransformByAspect(Map<String, Map<String, String>> fieldsByAspect)
      throws Exception {
    stubTransformByAspect(fieldsByAspect, Map.of());
  }

  private void stubTransformByAspect(
      Map<String, Map<String, String>> fieldsByAspect, Map<String, ObjectNode> objectsByAspect)
      throws Exception {
    when(searchDocumentTransformer.transformAspect(
            any(OperationContext.class),
            any(Urn.class),
            any(RecordTemplate.class),
            any(AspectSpec.class),
            anyBoolean(),
            any()))
        .thenAnswer(
            invocation -> {
              AspectSpec spec = invocation.getArgument(3);
              ObjectNode document = JsonNodeFactory.instance.objectNode();
              document.put("urn", testUrn.toString());
              fieldsByAspect.getOrDefault(spec.getName(), Map.of()).forEach(document::put);
              ObjectNode objects = objectsByAspect.get(spec.getName());
              if (objects != null) {
                objects
                    .fields()
                    .forEachRemaining(
                        entry -> document.set(entry.getKey(), entry.getValue().deepCopy()));
              }
              return Optional.of(document);
            });
  }

  private static ObjectNode objectField(String name, ObjectNode value) {
    ObjectNode fields = JsonNodeFactory.instance.objectNode();
    fields.set(name, value);
    return fields;
  }

  private static String nestedField(String... parts) {
    return String.join(ESSearchDAO.NESTED_FIELD_SEPARATOR, parts);
  }

  private JsonNode catchUpDocument(
      Map<String, SystemAspect> stored,
      Map<String, Map<String, String>> fieldsByAspect,
      Map<String, Set<String>> indexedFields)
      throws Exception {
    return catchUpDocument(stored, fieldsByAspect, Map.of(), indexedFields);
  }

  private JsonNode catchUpDocument(
      Map<String, SystemAspect> stored,
      Map<String, Map<String, String>> fieldsByAspect,
      Map<String, ObjectNode> objectsByAspect,
      Map<String, Set<String>> indexedFields)
      throws Exception {
    stubTransformByAspect(fieldsByAspect, objectsByAspect);
    when(elasticSearchService.getEsWriteDAO()).thenReturn(mock(ESWriteDAO.class));
    when(mockEvent.getMetadataChangeLog())
        .thenReturn(backfillChangeLog(BackfillHeaders.MODE_CATCH_UP, null));
    OperationContext replayContext = contextWithStoredAspects(stored);
    bindDatasetAspects(replayContext);
    String documentId = DigestUtils.sha256Hex(testUrn.toString());
    when(elasticSearchService.aspectFieldNamesBySearchGroup(any(), eq("dataset"), any()))
        .thenReturn(Map.of(documentId, indexedFields));

    strategy.processBatch(
        replayContext, Collections.singletonMap(testUrn, List.of(mockEvent)), true);
    V3SearchDocumentWrite documentWrite =
        v3Write(replayContext, groupIndex(replayContext, "dataset"), DOCUMENT_KINDS);
    assertEquals(documentWrite.docId(), documentId);
    return replayContext.getObjectMapper().readTree(documentWrite.source());
  }

  private Map<String, SystemAspect> storedDataset(String name, Map<String, RecordTemplate> extra) {
    Map<String, SystemAspect> stored = new HashMap<>(storedDataset(name));
    extra.forEach((aspectName, record) -> stored.put(aspectName, storedAspect(record)));
    return stored;
  }

  private static StructuredProperties structuredProperties() {
    PrimitivePropertyValueArray values = new PrimitivePropertyValueArray();
    values.add(PrimitivePropertyValue.create("kept"));
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:other"))
            .setValues(values);
    return new StructuredProperties()
        .setProperties(new StructuredPropertyValueAssignmentArray(assignment));
  }

  private Map<String, SystemAspect> storedDataset(String name) {
    EntitySpec datasetSpec =
        operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    return Map.of(
        "datasetKey",
        storedAspect(EntityKeyUtils.convertUrnToEntityKey(testUrn, datasetSpec.getKeyAspectSpec())),
        DATASET_PROPERTIES_ASPECT_NAME,
        storedAspect(new DatasetProperties().setName(name)));
  }

  private static SystemAspect storedAspect(RecordTemplate record) {
    SystemAspect stored = mock(SystemAspect.class);
    when(stored.getRecordTemplate()).thenReturn(record);
    when(stored.getSystemMetadata()).thenReturn(new SystemMetadata().setRunId("stored-run"));
    when(stored.getAuditStamp())
        .thenReturn(
            new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")));
    return stored;
  }
}
