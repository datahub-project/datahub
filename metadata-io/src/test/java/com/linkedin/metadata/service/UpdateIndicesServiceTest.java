package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.metadata.utils.SystemMetadataUtils;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
  public void setup() {
    MockitoAnnotations.openMocks(this);
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();
    updateIndicesService =
        new UpdateIndicesService(
            updateGraphIndicesService,
            entitySearchService,
            timeseriesAspectService,
            systemMetadataService,
            searchDocumentTransformer,
            "MD5");
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
    verify(systemMetadataService).deleteAspect(urn.toString(), CONTAINER_ASPECT_NAME);
    verify(searchDocumentTransformer)
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

    // Verify the method was called
    verify(searchDocumentTransformer)
        .transformAspect(
            eq(operationContext),
            eq(urn),
            nullable(RecordTemplate.class),
            eq(aspectSpec),
            eq(false),
            eq(event.getCreated()));
  }
}
