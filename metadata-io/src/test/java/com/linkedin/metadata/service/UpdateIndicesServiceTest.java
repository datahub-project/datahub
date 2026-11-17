package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROFILE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DEPRECATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.INCIDENT_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.incident.IncidentInfo;
import com.linkedin.incident.IncidentState;
import com.linkedin.incident.IncidentStatus;
import com.linkedin.incident.IncidentType;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.UpdateIndicesV3Strategy;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentWrite;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.StrictHandling;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.metadata.utils.TolerantHandling;
import com.linkedin.metadata.utils.UnknownDataHandlingsForTests;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateIndicesServiceTest {

  @Mock private UpdateGraphIndicesService updateGraphIndicesService;
  @Mock private ElasticSearchService entitySearchService;
  @Mock private TimeseriesAspectService timeseriesAspectService;
  @Mock private SystemMetadataService systemMetadataService;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;

  private OperationContext operationContext;
  private UpdateIndicesService updateIndicesService;

  @BeforeMethod
  public void tolerateUnknownDataByDefault() {
    UnknownDataHandlingsForTests.use(TolerantHandling.INSTANCE);
  }

  @AfterMethod
  public void resetUnknownDataHandling() {
    UnknownDataHandlingsForTests.reset();
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // V3 strategy drains pending bulk writes before rebuilding documents
    ESWriteDAO esWriteDAO = mock(ESWriteDAO.class);
    ESBulkProcessor esBulkProcessor = mock(ESBulkProcessor.class);
    when(entitySearchService.getEsWriteDAO()).thenReturn(esWriteDAO);
    when(esWriteDAO.bulkProcessors()).thenReturn(List.of(esBulkProcessor));
    when(esBulkProcessor.flushAndAwait()).thenReturn(BulkProcessorFlushResult.empty());

    // Create test strategies - for testing we'll create both V2 and V3 strategies
    UpdateIndicesV2Strategy v2Strategy =
        new UpdateIndicesV2Strategy(
            EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build(),
            entitySearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            null, // No semantic search config for this test
            mock(IndexConvention.class),
            false,
            mock(V2MappingsBuilder.class),
            null);

    UpdateIndicesV3Strategy v3Strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(
                EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
            .elasticSearchService(entitySearchService)
            .searchDocumentTransformer(searchDocumentTransformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(false)
            .timeseriesThrottleCache(null)
            .build();

    Collection<UpdateIndicesStrategy> strategies = Arrays.asList(v2Strategy, v3Strategy);

    updateIndicesService =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            strategies,
            null,
            true, // searchDiffMode
            true, // structuredPropertiesHookEnabled
            true); // structuredPropertiesWriteEnabled
  }

  @Test
  public void testDeleteV3TombstonesThrowsWhenTheDeleteDoesNotFinish() {
    when(entitySearchService.deleteV3Tombstones(any(), anyString()))
        .thenReturn(
            ESWriteDAO.DeleteByQueryResult.builder()
                .success(false)
                .remainingDocuments(3)
                .failureReason("timed out")
                .build());

    expectThrows(
        IllegalStateException.class,
        () -> updateIndicesService.deleteV3Tombstones(operationContext, "datasetindex_v3"));
  }

  @Test
  public void testIndicesFilledFromPrimaryStorageComeOnlyFromFillingStrategies() {
    UpdateIndicesStrategy filling = mock(UpdateIndicesStrategy.class);
    when(filling.isEnabled()).thenReturn(true);
    when(filling.fillsNewIndicesFromPrimaryStorage()).thenReturn(true);
    when(filling.getIndexMappings(operationContext))
        .thenReturn(
            List.of(
                MappingsBuilder.IndexMapping.builder()
                    .indexName("primaryindex_v3")
                    .mappings(Collections.emptyMap())
                    .build()));
    UpdateIndicesStrategy upserting = mock(UpdateIndicesStrategy.class);
    when(upserting.isEnabled()).thenReturn(true);
    when(upserting.fillsNewIndicesFromPrimaryStorage()).thenReturn(false);
    UpdateIndicesStrategy disabledFilling = mock(UpdateIndicesStrategy.class);
    when(disabledFilling.isEnabled()).thenReturn(false);
    when(disabledFilling.fillsNewIndicesFromPrimaryStorage()).thenReturn(true);

    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(filling, upserting, disabledFilling),
            null,
            true,
            true,
            true);

    assertEquals(
        service.indicesFilledFromPrimaryStorage(operationContext), Set.of("primaryindex_v3"));
    verify(upserting, never()).getIndexMappings(any(OperationContext.class));
    verify(disabledFilling, never()).getIndexMappings(any(OperationContext.class));
  }

  @Test
  public void testFillSearchIndicesWritesOnlyThroughFillingStrategies() {
    UpdateIndicesStrategy filling = mock(UpdateIndicesStrategy.class);
    when(filling.isEnabled()).thenReturn(true);
    when(filling.fillsNewIndicesFromPrimaryStorage()).thenReturn(true);
    UpdateIndicesStrategy upserting = mock(UpdateIndicesStrategy.class);
    when(upserting.isEnabled()).thenReturn(true);
    when(upserting.fillsNewIndicesFromPrimaryStorage()).thenReturn(false);
    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(filling, upserting),
            null,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,FilledDataset,PROD)");
    com.linkedin.container.Container container = new com.linkedin.container.Container();
    container.setContainer(UrnUtils.getUrn("urn:li:container:filled"));
    MetadataChangeLog event = new MetadataChangeLog();
    event.setChangeType(ChangeType.RESTATE);
    event.setEntityUrn(urn);
    event.setAspectName(CONTAINER_ASPECT_NAME);
    event.setEntityType(urn.getEntityType());
    event.setAspect(com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(container));
    event.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event.setCreated(AuditStampUtils.createDefaultAuditStamp());

    service.fillSearchIndices(operationContext, List.of(event));

    verify(filling)
        .processBatch(eq(operationContext), argThat(grouped -> grouped.containsKey(urn)), eq(true));
    verify(upserting, never()).processBatch(any(), any(), anyBoolean());
    // A fill rebuilds search documents only: no graph edges, no system metadata.
    verify(updateGraphIndicesService, never()).handleChangeEvent(any(), any());
    verify(systemMetadataService, never()).insert(any(), any(), any(), any());
  }

  @Test
  public void testContainerHandleDeleteEvent() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
    EntitySpec entitySpec = operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(CONTAINER_ASPECT_NAME);

    // Create test data
    MetadataChangeLog event = new MetadataChangeLog();
    event.setChangeType(ChangeType.DELETE);
    event.setEntityUrn(urn);
    event.setAspectName(CONTAINER_ASPECT_NAME);
    event.setEntityType(urn.getEntityType());
    event.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event.setCreated(AuditStampUtils.createDefaultAuditStamp());
    // Execute Delete
    updateIndicesService.handleChangeEvent(operationContext, event);

    // Verify
    verify(systemMetadataService)
        .deleteAspect(any(OperationContext.class), eq(urn.toString()), eq(CONTAINER_ASPECT_NAME));
    verify(searchDocumentTransformer, atLeastOnce())
        .transformAspect(
            eq(operationContext),
            eq(urn),
            nullable(RecordTemplate.class),
            eq(aspectSpec),
            eq(true),
            eq(event.getCreated()));
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, event);
  }

  @Test
  public void testHandleChangeEventsCollection() throws Exception {
    // Test the new handleChangeEvents method that takes a Collection
    Urn urn1 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset1,PROD)");
    Urn urn2 = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset2,PROD)");

    // Create proper aspect data
    com.linkedin.container.Container container1 = new com.linkedin.container.Container();
    container1.setContainer(UrnUtils.getUrn("urn:li:container:container1"));

    com.linkedin.container.Container container2 = new com.linkedin.container.Container();
    container2.setContainer(UrnUtils.getUrn("urn:li:container:container2"));

    MetadataChangeLog event1 = new MetadataChangeLog();
    event1.setChangeType(ChangeType.CREATE);
    event1.setEntityUrn(urn1);
    event1.setAspectName(CONTAINER_ASPECT_NAME);
    event1.setEntityType(urn1.getEntityType());
    event1.setAspect(com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(container1));
    event1.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event1.setCreated(AuditStampUtils.createDefaultAuditStamp());

    MetadataChangeLog event2 = new MetadataChangeLog();
    event2.setChangeType(ChangeType.UPSERT);
    event2.setEntityUrn(urn2);
    event2.setAspectName(CONTAINER_ASPECT_NAME);
    event2.setEntityType(urn2.getEntityType());
    event2.setAspect(com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(container2));
    event2.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event2.setCreated(AuditStampUtils.createDefaultAuditStamp());

    java.util.Collection<MetadataChangeLog> events = java.util.List.of(event1, event2);

    // Execute batch processing
    updateIndicesService.handleChangeEvents(operationContext, events);

    // Verify both events were processed
    verify(updateGraphIndicesService, times(2))
        .handleChangeEvent(eq(operationContext), any(MetadataChangeLog.class));
  }

  @Test
  public void testEmptySearchDocumentLogging() throws Exception {
    // Test the change from log.info to log.debug for empty search documents
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,EmptyDataset,PROD)");
    EntitySpec entitySpec = operationContext.getEntityRegistry().getEntitySpec(DATASET_ENTITY_NAME);
    AspectSpec aspectSpec = entitySpec.getAspectSpec(CONTAINER_ASPECT_NAME);

    // Create proper aspect data
    com.linkedin.container.Container container = new com.linkedin.container.Container();
    container.setContainer(UrnUtils.getUrn("urn:li:container:empty"));

    MetadataChangeLog event = new MetadataChangeLog();
    event.setChangeType(ChangeType.CREATE);
    event.setEntityUrn(urn);
    event.setAspectName(CONTAINER_ASPECT_NAME);
    event.setEntityType(urn.getEntityType());
    event.setAspect(com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect(container));
    event.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event.setCreated(AuditStampUtils.createDefaultAuditStamp());

    // Mock empty search document
    when(searchDocumentTransformer.transformAspect(
            eq(operationContext),
            eq(urn),
            nullable(RecordTemplate.class),
            eq(aspectSpec),
            eq(false),
            eq(event.getCreated())))
        .thenReturn(java.util.Optional.empty());

    // Execute - this should trigger the empty document logging
    updateIndicesService.handleChangeEvent(operationContext, event);

    // Verify the method was called twice (once by V2 strategy, once by V3 strategy)
    verify(searchDocumentTransformer, times(2))
        .transformAspect(
            eq(operationContext),
            eq(urn),
            nullable(RecordTemplate.class),
            eq(aspectSpec),
            eq(false),
            eq(event.getCreated()));
  }

  @Test
  public void testThrottle_ServiceAcceptsNonNullThrottleCache() {
    TimeseriesWriteThrottleCache cache =
        new TimeseriesWriteThrottleCache(
            com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration.builder()
                .entityIndex(
                    com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                        .IndexThrottleConfig.builder()
                        .enabled(true)
                        .build())
                .refreshPeriodSeconds(3600)
                .maxCacheUrns(10_000)
                .build());

    UpdateIndicesV2Strategy v2Strategy =
        new UpdateIndicesV2Strategy(
            EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build(),
            entitySearchService,
            searchDocumentTransformer,
            timeseriesAspectService,
            "MD5",
            null,
            mock(IndexConvention.class),
            false,
            mock(V2MappingsBuilder.class),
            cache);

    UpdateIndicesService serviceWithThrottle =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            Collections.singletonList(v2Strategy),
            cache,
            true,
            true,
            true);

    assertTrue(cache.isEnabled());
    assertTrue(cache.isEntityIndexEnabled());
  }

  @Test
  public void testHandleChangeEvents_WithBothStrategies() throws Exception {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");

    // Create test data
    MetadataChangeLog event = new MetadataChangeLog();
    event.setChangeType(ChangeType.UPSERT);
    event.setEntityUrn(urn);
    event.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    event.setEntityType(urn.getEntityType());
    event.setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
    event.setCreated(AuditStampUtils.createDefaultAuditStamp());

    // Create aspect data for UPSERT operation
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setDescription("Test dataset description");
    event.setAspect(GenericRecordUtils.serializeAspect(datasetProperties));

    // Execute with both V2 and V3 strategies
    updateIndicesService.handleChangeEvents(operationContext, Collections.singletonList(event));

    // Verify both strategies were called (V2 through individual methods, V3 through processBatch)
    // The exact verification depends on the implementation details, but both should process the
    // event
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, event);
  }

  @Test
  public void testThrowingStrategyDoesNotSkipRemainingStrategyOrSideEffects() {
    UpdateIndicesStrategy throwingStrategy = mock(UpdateIndicesStrategy.class);
    UpdateIndicesStrategy remainingStrategy = mock(UpdateIndicesStrategy.class);
    when(throwingStrategy.isEnabled()).thenReturn(true);
    when(remainingStrategy.isEnabled()).thenReturn(true);
    RuntimeException strategyFailure = new RuntimeException("V3 projection failed");
    doThrow(strategyFailure)
        .when(throwingStrategy)
        .processBatch(eq(operationContext), any(), eq(true));

    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(throwingStrategy, remainingStrategy),
            null,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,strategy.failure,PROD)");
    DatasetProperties datasetProperties =
        new DatasetProperties().setDescription("still apply side effects");
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setEntityType(urn.getEntityType())
            .setAspect(GenericRecordUtils.serializeAspect(datasetProperties))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp());

    RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> service.handleChangeEvent(operationContext, event));

    assertSame(thrown, strategyFailure);
    verify(remainingStrategy).processBatch(eq(operationContext), any(), eq(true));
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, event);
    verify(systemMetadataService)
        .insert(
            eq(operationContext),
            eq(event.getSystemMetadata()),
            eq(urn.toString()),
            eq(DATASET_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testRetryReplaysIdenticalGraphAndSystemSideEffects() {
    UpdateIndicesStrategy retryingStrategy = mock(UpdateIndicesStrategy.class);
    when(retryingStrategy.isEnabled()).thenReturn(true);
    RetryableIndexingException retryableFailure =
        new RetryableIndexingException(
            "transient storage failure", new RuntimeException("connection refused"));
    doThrow(retryableFailure)
        .doNothing()
        .when(retryingStrategy)
        .processBatch(eq(operationContext), any(), eq(true));

    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(retryingStrategy),
            null,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,retry.replay,PROD)");
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setEntityType(urn.getEntityType())
            .setAspect(
                GenericRecordUtils.serializeAspect(
                    new DatasetProperties().setDescription("idempotent replay")))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp());

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () -> service.handleChangeEvent(operationContext, event));
    assertSame(thrown, retryableFailure);

    service.handleChangeEvent(operationContext, event);

    verify(retryingStrategy, times(2)).processBatch(eq(operationContext), any(), eq(true));
    verify(updateGraphIndicesService, times(2)).handleChangeEvent(operationContext, event);
    verify(systemMetadataService, times(2))
        .insert(
            eq(operationContext),
            eq(event.getSystemMetadata()),
            eq(urn.toString()),
            eq(DATASET_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testRetryableFailureTakesPrecedenceOverOtherStrategyFailures() {
    UpdateIndicesStrategy terminalStrategy = mock(UpdateIndicesStrategy.class);
    UpdateIndicesStrategy retryingStrategy = mock(UpdateIndicesStrategy.class);
    when(terminalStrategy.isEnabled()).thenReturn(true);
    when(retryingStrategy.isEnabled()).thenReturn(true);
    IllegalArgumentException terminalFailure = new IllegalArgumentException("malformed projection");
    RetryableIndexingException retryableFailure =
        new RetryableIndexingException(
            "transient storage failure", new RuntimeException("connection refused"));
    doThrow(terminalFailure)
        .when(terminalStrategy)
        .processBatch(eq(operationContext), any(), eq(true));
    doThrow(retryableFailure)
        .when(retryingStrategy)
        .processBatch(eq(operationContext), any(), eq(true));
    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(terminalStrategy, retryingStrategy),
            null,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,retry.precedence,PROD)");
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setEntityType(urn.getEntityType())
            .setAspect(
                GenericRecordUtils.serializeAspect(
                    new DatasetProperties().setDescription("retry precedence")))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp());

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () -> service.handleChangeEvent(operationContext, event));

    assertSame(thrown, retryableFailure);
    assertTrue(thrown.getSuppressed().length == 1);
    assertSame(thrown.getSuppressed()[0], terminalFailure);
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, event);
  }

  @Test
  public void testSideEffectFailureDoesNotClobberSavedRetryable() {
    // A strategy records a retryable V3 write loss, then a required side effect (the graph write)
    // fails synchronously. The synchronous failure must not propagate straight out and bypass the
    // deferred rethrow — that would replace the retryable with a non-retryable exception and let
    // the
    // consumer commit the offset past the lost V3 document. The retryable must survive as the
    // primary cause, with the side-effect failure suppressed.
    UpdateIndicesStrategy retryingStrategy = mock(UpdateIndicesStrategy.class);
    when(retryingStrategy.isEnabled()).thenReturn(true);
    RetryableIndexingException retryableFailure =
        new RetryableIndexingException(
            "V3 write lost transiently", new RuntimeException("es_rejected_execution_exception"));
    doThrow(retryableFailure)
        .when(retryingStrategy)
        .processBatch(eq(operationContext), any(), eq(true));

    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(retryingStrategy),
            null,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,sideeffect.failure,PROD)");
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn)
            .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
            .setEntityType(urn.getEntityType())
            .setAspect(
                GenericRecordUtils.serializeAspect(
                    new DatasetProperties().setDescription("side effect throws")))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp());
    IllegalStateException sideEffectFailure = new IllegalStateException("graph write rejected");
    doThrow(sideEffectFailure)
        .when(updateGraphIndicesService)
        .handleChangeEvent(eq(operationContext), any(MetadataChangeLog.class));

    RetryableIndexingException thrown =
        expectThrows(
            RetryableIndexingException.class,
            () -> service.handleChangeEvent(operationContext, event));

    assertSame(thrown, retryableFailure);
    assertTrue(Arrays.asList(thrown.getSuppressed()).contains(sideEffectFailure));
  }

  @Test
  public void testThrowingStrategySkipsThrottleAccounting() {
    UpdateIndicesStrategy throwingStrategy = mock(UpdateIndicesStrategy.class);
    when(throwingStrategy.isEnabled()).thenReturn(true);
    doThrow(new RuntimeException("V3 projection failed"))
        .when(throwingStrategy)
        .processBatch(eq(operationContext), any(), eq(true));
    TimeseriesWriteThrottleCache throttleCache =
        new TimeseriesWriteThrottleCache(
            com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration.builder()
                .entityIndex(
                    com.linkedin.metadata.config.search.TimeseriesWriteThrottleConfiguration
                        .IndexThrottleConfig.builder()
                        .enabled(true)
                        .build())
                .refreshPeriodSeconds(3600)
                .maxCacheUrns(100)
                .build());
    UpdateIndicesService service =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            systemMetadataService,
            List.of(throwingStrategy),
            throttleCache,
            true,
            true,
            true);
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,throttle.accounting,PROD)");
    long eventTime = 1_000_000L;
    MetadataChangeLog event =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(urn)
            .setAspectName(DATASET_PROFILE_ASPECT_NAME)
            .setEntityType(urn.getEntityType())
            .setAspect(
                GenericRecordUtils.serializeAspect(
                    new DatasetProfile().setTimestampMillis(eventTime)))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp().setTime(eventTime));

    expectThrows(RuntimeException.class, () -> service.handleChangeEvent(operationContext, event));

    assertFalse(
        throttleCache.shouldThrottle(
            DATASET_ENTITY_NAME, urn.toString(), DATASET_PROFILE_ASPECT_NAME, eventTime + 1));
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, event);
  }

  @Test
  public void testFlushAndAwaitDrainsEveryClusterProcessor() {
    ESBulkProcessor primary = mock(ESBulkProcessor.class);
    ESBulkProcessor secondary = mock(ESBulkProcessor.class);
    when(primary.flushAndAwait()).thenReturn(BulkProcessorFlushResult.empty());
    when(secondary.flushAndAwait())
        .thenReturn(new BulkProcessorFlushResult(7, 1, 0, 0, 0, false, null));
    ESWriteDAO esWriteDAO = mock(ESWriteDAO.class);
    when(esWriteDAO.bulkProcessors()).thenReturn(List.of(primary, secondary));
    when(entitySearchService.getEsWriteDAO()).thenReturn(esWriteDAO);

    BulkProcessorFlushResult result = updateIndicesService.flushAndAwait();

    verify(primary).flushAndAwait();
    verify(secondary).flushAndAwait();
    assertEquals(result.successfulV3Writes(), 7);
    assertEquals(result.staleVersionRejected(), 1);
  }

  @Test
  public void testHandleChangeEvents_SkipsEventsUnknownToRegistry() {
    MetadataChangeLog knownFirst = datasetPropertiesEvent("known.first");
    MetadataChangeLog unknownAspect =
        datasetPropertiesEvent("unknown.aspect").setAspectName("aspectFromNewerBuild");
    Urn unknownEntityUrn = UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc");
    MetadataChangeLog unknownEntity =
        datasetPropertiesEvent("unknown.entity")
            .setEntityUrn(unknownEntityUrn)
            .setEntityType(unknownEntityUrn.getEntityType());
    MetadataChangeLog knownLast = datasetPropertiesEvent("known.last");

    updateIndicesService.handleChangeEvents(
        operationContext, List.of(knownFirst, unknownAspect, unknownEntity, knownLast));

    verify(updateGraphIndicesService).handleChangeEvent(operationContext, knownFirst);
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, knownLast);
    verify(updateGraphIndicesService, never()).handleChangeEvent(operationContext, unknownAspect);
    verify(updateGraphIndicesService, never()).handleChangeEvent(operationContext, unknownEntity);
  }

  @Test
  public void testHandleChangeEvents_CountsEventsUnknownToRegistry() {
    // Skips are counted so operators can see how much a newer version's data is being ignored.
    com.linkedin.metadata.utils.metrics.MetricUtils metrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    OperationContext context = org.mockito.Mockito.spy(operationContext);
    when(context.getMetricUtils()).thenReturn(java.util.Optional.of(metrics));
    MetadataChangeLog unknownAspect =
        datasetPropertiesEvent("unknown.metric").setAspectName("aspectFromNewerBuild");

    updateIndicesService.handleChangeEvents(context, List.of(unknownAspect));

    verify(metrics).increment(UpdateIndicesService.class, "unknown_to_registry_skipped", 1);
  }

  @Test
  public void testHandleChangeEvents_CountsInvalidEventsFromANewerVersion() {
    com.linkedin.metadata.utils.metrics.MetricUtils metrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    OperationContext context = org.mockito.Mockito.spy(operationContext);
    when(context.getMetricUtils()).thenReturn(java.util.Optional.of(metrics));
    MetadataChangeLog unstrippable =
        datasetAspectEvent(
            "invalid.metric",
            DEPRECATION_ASPECT_NAME,
            new Deprecation()
                .setDeprecated(true)
                .setNote("moved")
                .setActor(UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc")));

    updateIndicesService.handleChangeEvents(context, List.of(unstrippable));

    verify(metrics).increment(UpdateIndicesService.class, "invalid_mcl_skipped", 1);
  }

  @Test
  public void testHandleChangeEvents_SkipsEventsWhoseUrnKeyReferencesAnUnknownType() {
    // A schema field of an entity type only a newer version has fails key validation; it is
    // skipped on its own instead of failing the whole batch.
    MetadataChangeLog known = datasetPropertiesEvent("nested.known");
    Urn nested = UrnUtils.getUrn("urn:li:schemaField:(urn:li:entityFromNewerBuild:x,col_a)");
    MetadataChangeLog nestedUnknown =
        new MetadataChangeLog()
            .setChangeType(ChangeType.UPSERT)
            .setEntityUrn(nested)
            .setEntityType(nested.getEntityType())
            .setAspectName(STATUS_ASPECT_NAME)
            .setAspect(GenericRecordUtils.serializeAspect(new Status().setRemoved(false)))
            .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
            .setCreated(AuditStampUtils.createDefaultAuditStamp());

    updateIndicesService.handleChangeEvents(operationContext, List.of(nestedUnknown, known));

    verify(updateGraphIndicesService).handleChangeEvent(operationContext, known);
    verify(updateGraphIndicesService, never()).handleChangeEvent(operationContext, nestedUnknown);
  }

  @Test
  public void testHandleChangeEvents_SkipsOnlyEventsFailingValidation() {
    // A known aspect referencing an entity type this registry does not know (written by a newer
    // version): the reference is stripped and the rest indexed. When the reference sits in a
    // required top-level field it can't be stripped, so only that event is dropped, not the batch.
    MetadataChangeLog knownFirst = datasetPropertiesEvent("known.first");
    Urn unknownTypeUrn = UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc");
    MetadataChangeLog strippable =
        datasetAspectEvent(
            "strippable",
            DOMAINS_ASPECT_NAME,
            new Domains()
                .setDomains(
                    new UrnArray(unknownTypeUrn, UrnUtils.getUrn("urn:li:domain:finance"))));
    MetadataChangeLog unstrippable =
        datasetAspectEvent(
            "unstrippable",
            DEPRECATION_ASPECT_NAME,
            new Deprecation().setDeprecated(true).setNote("moved").setActor(unknownTypeUrn));
    MetadataChangeLog knownLast = datasetPropertiesEvent("known.last");

    updateIndicesService.handleChangeEvents(
        operationContext, List.of(knownFirst, strippable, unstrippable, knownLast));

    verify(updateGraphIndicesService).handleChangeEvent(operationContext, knownFirst);
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, strippable);
    verify(updateGraphIndicesService).handleChangeEvent(operationContext, knownLast);
    verify(updateGraphIndicesService, never()).handleChangeEvent(operationContext, unstrippable);
  }

  private static MetadataChangeLog datasetAspectEvent(
      String name, String aspectName, RecordTemplate aspect) {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs," + name + ",PROD)");
    return new MetadataChangeLog()
        .setChangeType(ChangeType.UPSERT)
        .setEntityUrn(urn)
        .setEntityType(urn.getEntityType())
        .setAspectName(aspectName)
        .setAspect(GenericRecordUtils.serializeAspect(aspect))
        .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
        .setCreated(AuditStampUtils.createDefaultAuditStamp());
  }

  @Test
  public void testHandleChangeEvents_StrictHandlingFailsOnInvalidEventsFromANewerVersion() {
    // Strict handling keeps the original behavior: the whole batch fails, nothing is counted.
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    com.linkedin.metadata.utils.metrics.MetricUtils metrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    OperationContext context = org.mockito.Mockito.spy(operationContext);
    when(context.getMetricUtils()).thenReturn(java.util.Optional.of(metrics));
    MetadataChangeLog unstrippable =
        datasetAspectEvent(
            "strict.invalid",
            DEPRECATION_ASPECT_NAME,
            new Deprecation()
                .setDeprecated(true)
                .setNote("moved")
                .setActor(UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc")));

    expectThrows(
        ValidationException.class,
        () -> updateIndicesService.handleChangeEvents(context, List.of(unstrippable)));
    verify(metrics, never()).increment(UpdateIndicesService.class, "invalid_mcl_skipped", 1);
  }

  @Test
  public void testHandleChangeEvents_EnumToleranceAloneFailsOnUnknownReferences() {
    // References to unknown entity types are the unknown-data flag's kind; tolerating only enum
    // values leaves an event that can't be stripped failing the batch, as before.
    UnknownDataHandlingsForTests.use(TolerantHandling.of(false, true, entityType -> true));
    MetadataChangeLog unstrippable =
        datasetAspectEvent(
            "enumonly.invalid",
            DEPRECATION_ASPECT_NAME,
            new Deprecation()
                .setDeprecated(true)
                .setNote("moved")
                .setActor(UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc")));

    expectThrows(
        ValidationException.class,
        () -> updateIndicesService.handleChangeEvents(operationContext, List.of(unstrippable)));
  }

  @Test
  public void testHandleChangeEvents_InvalidPayloadNotFromNewerVersionStillFails() {
    // Only payloads a newer version wrote are dropped alone; other invalid data fails as before,
    // so it can't silently go unindexed.
    Deprecation invalid = new Deprecation().setDeprecated(true).setNote("n");
    invalid.data().put("actor", "not-a-urn");
    MetadataChangeLog event = datasetAspectEvent("invalid", DEPRECATION_ASPECT_NAME, invalid);

    expectThrows(
        ValidationException.class,
        () -> updateIndicesService.handleChangeEvents(operationContext, List.of(event)));
  }

  @Test
  public void testHandleChangeEvents_IndexesTheNewValueWhenThePreviousIsFromANewerVersion()
      throws Exception {
    // A change away from an owner type only a newer version defines: the new value is valid, the
    // previous one fails validation here. The new value is indexed without the previous value, and
    // the graph gets that previous value to diff against.
    com.linkedin.metadata.utils.metrics.MetricUtils metrics =
        mock(com.linkedin.metadata.utils.metrics.MetricUtils.class);
    OperationContext context = org.mockito.Mockito.spy(operationContext);
    when(context.getMetricUtils()).thenReturn(java.util.Optional.of(metrics));
    MetadataChangeLog previousFromNewerVersion =
        ownershipChangeAwayFromNewerOwnerType("prev.newer");
    MetadataChangeLog known = datasetPropertiesEvent("prev.known");

    updateIndicesService.handleChangeEvents(context, List.of(previousFromNewerVersion, known));

    ArgumentCaptor<MetadataChangeLog> indexed = ArgumentCaptor.forClass(MetadataChangeLog.class);
    ArgumentCaptor<RecordTemplate> previous = ArgumentCaptor.forClass(RecordTemplate.class);
    verify(updateGraphIndicesService)
        .handleChangeEventWithUnvalidatedPrevious(
            eq(context), indexed.capture(), previous.capture());
    assertFalse(indexed.getValue().hasPreviousAspectValue());
    assertEquals(indexed.getValue().getAspect(), previousFromNewerVersion.getAspect());
    assertEquals(
        new Ownership(previous.getValue().data()).getOwners().get(0).data().get("type"),
        "OWNER_TYPE_FROM_NEWER_BUILD");
    verify(updateGraphIndicesService).handleChangeEvent(context, known);
    verify(updateGraphIndicesService, never()).handleChangeEvent(context, previousFromNewerVersion);
    verify(searchDocumentTransformer, atLeastOnce())
        .transformAspect(
            eq(context),
            eq(previousFromNewerVersion.getEntityUrn()),
            argThat(aspect -> aspect instanceof Ownership),
            any(AspectSpec.class),
            eq(false),
            any());
    verify(metrics)
        .increment(UpdateIndicesService.class, "mcl_previous_from_newer_version_omitted", 1);
    verify(metrics, never()).increment(UpdateIndicesService.class, "invalid_mcl_skipped", 1);
  }

  @Test
  public void testHandleChangeEvents_DeletesWhenThePreviousValueIsFromANewerVersion()
      throws Exception {
    // Deleting an aspect whose stored value holds an owner type only a newer version defines: the
    // delete is indexed, and the graph gets the previous value to remove its edges.
    MetadataChangeLog delete =
        ownershipChangeAwayFromNewerOwnerType("prev.newer.delete").setChangeType(ChangeType.DELETE);
    delete.removeAspect();

    updateIndicesService.handleChangeEvents(operationContext, List.of(delete));

    verify(updateGraphIndicesService)
        .handleChangeEventWithUnvalidatedPrevious(
            eq(operationContext),
            argThat(
                mcl -> mcl.getChangeType() == ChangeType.DELETE && !mcl.hasPreviousAspectValue()),
            argThat(previous -> previous instanceof Ownership));
    verify(searchDocumentTransformer, atLeastOnce())
        .transformAspect(
            eq(operationContext),
            eq(delete.getEntityUrn()),
            nullable(RecordTemplate.class),
            any(AspectSpec.class),
            eq(true),
            any());
    verify(systemMetadataService)
        .deleteAspect(
            any(OperationContext.class),
            eq(delete.getEntityUrn().toString()),
            eq(OWNERSHIP_ASPECT_NAME));
    verify(updateGraphIndicesService, never())
        .handleChangeEvent(any(OperationContext.class), any(MetadataChangeLog.class));
  }

  @Test
  public void testHandleChangeEvents_UpsertClearsFieldsOnlyThePreviousValueFromANewerVersionHad()
      throws Exception {
    // The previous value has an incident type only a newer version defines, and only it has a
    // description and priority. The search diff still reads it, so both fields are cleared.
    Urn urn = UrnUtils.getUrn("urn:li:incident:prev-newer-upsert");
    IncidentInfo current = incidentInfo();
    IncidentInfo previous = incidentInfo();
    previous.setDescription("old description").setPriority(1);
    previous.data().put("type", "INCIDENT_TYPE_FROM_NEWER_BUILD");
    MetadataChangeLog upsert =
        incidentInfoEvent(urn, ChangeType.UPSERT)
            .setAspect(GenericRecordUtils.serializeAspect(current))
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(previous))
            .setPreviousSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());

    serviceTransformingDocuments().handleChangeEvents(operationContext, List.of(upsert));

    JsonNode v2 = v2DocumentWritten();
    assertEquals(v2.get("title").asText(), "incident");
    assertTrue(v2.path("description").isNull());
    assertTrue(v2.path("priority").isNull());
    JsonNode v3 = v3DocumentWritten();
    assertEquals(v3.get("title").asText(), "incident");
    assertTrue(v3.path("description").isNull());
    assertTrue(v3.path("priority").isNull());
    assertTrue(v3.path("_aspects").path(INCIDENT_INFO_ASPECT_NAME).path("description").isNull());
    verify(updateGraphIndicesService)
        .handleChangeEventWithUnvalidatedPrevious(
            eq(operationContext),
            argThat(mcl -> !mcl.hasPreviousAspectValue()),
            argThat(value -> "INCIDENT_TYPE_FROM_NEWER_BUILD".equals(value.data().get("type"))));
  }

  @Test
  public void testHandleChangeEvents_DeleteClearsFieldsWhenThePreviousValueIsFromANewerVersion()
      throws Exception {
    // The aspect is deleted while its stored value has an incident type only a newer version
    // defines: the documents no longer hold any of its values.
    Urn urn = UrnUtils.getUrn("urn:li:incident:prev-newer-delete");
    IncidentInfo previous = incidentInfo();
    previous.setDescription("old description").setPriority(1);
    previous.data().put("type", "INCIDENT_TYPE_FROM_NEWER_BUILD");
    MetadataChangeLog delete =
        incidentInfoEvent(urn, ChangeType.DELETE)
            .setPreviousAspectValue(GenericRecordUtils.serializeAspect(previous))
            .setPreviousSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());

    serviceTransformingDocuments().handleChangeEvents(operationContext, List.of(delete));

    JsonNode v2 = v2DocumentWritten();
    for (String field : List.of("title", "description", "priority", "type")) {
      assertTrue(v2.path(field).isNull(), field);
    }
    JsonNode v3 = v3DocumentWritten();
    for (String field : List.of("title", "description", "priority", "type")) {
      assertFalse(v3.hasNonNull(field), field);
      assertFalse(v3.path("_aspects").path(INCIDENT_INFO_ASPECT_NAME).hasNonNull(field), field);
    }
  }

  /** V2 and V3 strategies that build documents with the real transformer. */
  private UpdateIndicesService serviceTransformingDocuments() {
    SearchDocumentTransformer transformer =
        new SearchDocumentTransformer(1000, 1000, 1000, false, ESUtils.KEYWORD_MAXLENGTH);
    UpdateIndicesV2Strategy v2Strategy =
        new UpdateIndicesV2Strategy(
            EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build(),
            entitySearchService,
            transformer,
            timeseriesAspectService,
            "MD5",
            null,
            mock(IndexConvention.class),
            false,
            mock(V2MappingsBuilder.class),
            null);
    UpdateIndicesV3Strategy v3Strategy =
        UpdateIndicesV3Strategy.builder()
            .v3Config(
                EntityIndexVersionConfiguration.builder().enabled(true).cleanup(false).build())
            .elasticSearchService(entitySearchService)
            .searchDocumentTransformer(transformer)
            .timeseriesAspectService(timeseriesAspectService)
            .v2Enabled(true)
            .build();
    return new UpdateIndicesService(
        updateGraphIndicesService,
        entitySearchService,
        systemMetadataService,
        List.of(v2Strategy, v3Strategy),
        null,
        true,
        true,
        true);
  }

  private JsonNode v2DocumentWritten() throws Exception {
    ArgumentCaptor<String> document = ArgumentCaptor.forClass(String.class);
    verify(entitySearchService)
        .upsertDocument(any(OperationContext.class), eq("incident"), document.capture(), any());
    return new ObjectMapper().readTree(document.getValue());
  }

  private JsonNode v3DocumentWritten() throws Exception {
    ArgumentCaptor<V3SearchDocumentWrite> write =
        ArgumentCaptor.forClass(V3SearchDocumentWrite.class);
    verify(entitySearchService).writeV3SearchDocument(any(OperationContext.class), write.capture());
    return new ObjectMapper().readTree(write.getValue().source());
  }

  private static IncidentInfo incidentInfo() {
    return new IncidentInfo()
        .setType(IncidentType.OPERATIONAL)
        .setTitle("incident")
        .setEntities(
            new UrnArray(UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,x,PROD)")))
        .setStatus(
            new IncidentStatus()
                .setState(IncidentState.ACTIVE)
                .setLastUpdated(AuditStampUtils.createDefaultAuditStamp()))
        .setCreated(AuditStampUtils.createDefaultAuditStamp());
  }

  private static MetadataChangeLog incidentInfoEvent(Urn urn, ChangeType changeType) {
    return new MetadataChangeLog()
        .setChangeType(changeType)
        .setEntityUrn(urn)
        .setEntityType(urn.getEntityType())
        .setAspectName(INCIDENT_INFO_ASPECT_NAME)
        .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
        .setCreated(AuditStampUtils.createDefaultAuditStamp());
  }

  @Test
  public void testHandleChangeEvents_StrictHandlingFailsWhenThePreviousValueIsFromANewerVersion() {
    UnknownDataHandlingsForTests.use(StrictHandling.INSTANCE);
    MetadataChangeLog event = ownershipChangeAwayFromNewerOwnerType("prev.strict");

    expectThrows(
        ValidationException.class,
        () -> updateIndicesService.handleChangeEvents(operationContext, List.of(event)));
  }

  @Test
  public void testHandleChangeEvents_InvalidNewValueStillFailsWhenThePreviousIsFromANewerVersion() {
    // The previous value is the newer version's, but the new one is invalid for another reason:
    // the event fails as before instead of being dropped.
    Deprecation invalid = new Deprecation().setDeprecated(true).setNote("n");
    invalid.data().put("actor", "not-a-urn");
    MetadataChangeLog event =
        datasetAspectEvent("prev.invalid.new", DEPRECATION_ASPECT_NAME, invalid)
            .setPreviousAspectValue(
                GenericRecordUtils.serializeAspect(
                    new Deprecation()
                        .setDeprecated(true)
                        .setNote("moved")
                        .setActor(UrnUtils.getUrn("urn:li:entityFromNewerBuild:abc"))));

    expectThrows(
        ValidationException.class,
        () -> updateIndicesService.handleChangeEvents(operationContext, List.of(event)));
  }

  private static MetadataChangeLog ownershipChangeAwayFromNewerOwnerType(String name) {
    Owner previousOwner =
        new Owner().setOwner(UrnUtils.getUrn("urn:li:corpuser:alice")).setType(OwnershipType.NONE);
    // An owner type this build doesn't define, as a newer version wrote it.
    previousOwner.data().put("type", "OWNER_TYPE_FROM_NEWER_BUILD");
    Ownership previous = new Ownership().setOwners(new OwnerArray(previousOwner));
    Ownership current =
        new Ownership()
            .setOwners(
                new OwnerArray(
                    new Owner()
                        .setOwner(UrnUtils.getUrn("urn:li:corpuser:alice"))
                        .setType(OwnershipType.TECHNICAL_OWNER)));
    return datasetAspectEvent(name, OWNERSHIP_ASPECT_NAME, current)
        .setPreviousAspectValue(GenericRecordUtils.serializeAspect(previous))
        .setPreviousSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata());
  }

  @Test
  public void testHandleChangeEvents_MalformedKnownEventStillFails() {
    // Only unknown entities/aspects are skipped; other build failures must still surface
    MetadataChangeLog missingAspectPayload = datasetPropertiesEvent("missing.payload");
    missingAspectPayload.removeAspect();

    expectThrows(
        UnsupportedOperationException.class,
        () ->
            updateIndicesService.handleChangeEvents(
                operationContext, List.of(missingAspectPayload)));
    verify(updateGraphIndicesService, never())
        .handleChangeEvent(eq(operationContext), any(MetadataChangeLog.class));
  }

  @Test
  public void testHandleChangeEvents_MissingAspectNameStillFails() {
    // An MCL without an aspect name is malformed, not unknown — it must not be silently skipped
    MetadataChangeLog missingAspectName = datasetPropertiesEvent("missing.aspect.name");
    missingAspectName.removeAspectName();

    expectThrows(
        UnsupportedOperationException.class,
        () ->
            updateIndicesService.handleChangeEvents(operationContext, List.of(missingAspectName)));
    verify(updateGraphIndicesService, never())
        .handleChangeEvent(eq(operationContext), any(MetadataChangeLog.class));
  }

  private static MetadataChangeLog datasetPropertiesEvent(String name) {
    Urn urn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs," + name + ",PROD)");
    return new MetadataChangeLog()
        .setChangeType(ChangeType.UPSERT)
        .setEntityUrn(urn)
        .setAspectName(DATASET_PROPERTIES_ASPECT_NAME)
        .setEntityType(urn.getEntityType())
        .setAspect(GenericRecordUtils.serializeAspect(new DatasetProperties().setDescription(name)))
        .setSystemMetadata(SystemMetadataUtils.createDefaultSystemMetadata())
        .setCreated(AuditStampUtils.createDefaultAuditStamp());
  }
}
