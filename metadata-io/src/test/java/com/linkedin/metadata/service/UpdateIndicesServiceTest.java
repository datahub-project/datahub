package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
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
  @Mock private EntitySearchService entitySearchService;
  @Mock private TimeseriesAspectService timeseriesAspectService;
  @Mock private SystemMetadataService systemMetadataService;
  @Mock private SearchDocumentTransformer searchDocumentTransformer;
  @Mock private EntityIndexBuilders entityIndexBuilders;

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
            entityIndexBuilders,
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
}
