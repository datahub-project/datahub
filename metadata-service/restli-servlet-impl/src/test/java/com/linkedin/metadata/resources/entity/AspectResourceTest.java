package com.linkedin.metadata.resources.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.IngestAspectsResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;

import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import mock.MockEntityRegistry;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AspectResourceTest {
  private AspectResource aspectResource;
  private EntityService<?> entityService;
  private AspectDao aspectDao;
  private EventProducer producer;
  private EntityRegistry entityRegistry;
  private UpdateIndicesService updateIndicesService;
  private PreProcessHooks preProcessHooks;
  private Authorizer authorizer;
  private OperationContext opContext;

  @BeforeTest
  public void setup() {
    aspectResource = new AspectResource();
    aspectDao = mock(AspectDao.class);
    producer = mock(EventProducer.class);
    updateIndicesService = mock(UpdateIndicesService.class);
    preProcessHooks = mock(PreProcessHooks.class);
    entityService = new EntityServiceImpl(aspectDao, producer, false,
            preProcessHooks, true);
    entityService.setUpdateIndicesService(updateIndicesService);
    authorizer = mock(Authorizer.class);
    aspectResource.setAuthorizer(authorizer);
    aspectResource.setEntityService(entityService);
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    aspectResource.setSystemOperationContext(opContext);
    aspectResource.setEntitySearchService(mock(EntitySearchService.class));
    entityRegistry = opContext.getEntityRegistry();
  }

  @Test
  public void testAsyncDefaultAspects() throws URISyntaxException {
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    Urn urn = new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD);
    mcp.setEntityUrn(urn);
    DatasetProperties properties = new DatasetProperties().setName("name");
    mcp.setAspect(GenericRecordUtils.serializeAspect(properties));
    mcp.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);
    aspectResource.ingestProposal(mcp, "true");
    verify(producer, times(1)).produceMetadataChangeProposal(any(OperationContext.class), eq(urn),
            argThat(arg -> arg.getMetadataChangeProposal().equals(mcp)));
    verifyNoMoreInteractions(producer);
    verifyNoMoreInteractions(aspectDao);

    reset(producer, aspectDao);

    ChangeItemImpl req = ChangeItemImpl.builder()
            .urn(urn)
            .aspectName(mcp.getAspectName())
            .recordTemplate(mcp.getAspect())
            .auditStamp(new AuditStamp())
            .metadataChangeProposal(mcp)
            .build(opContext.getAspectRetriever());
    IngestAspectsResult txResult = IngestAspectsResult.builder()
            .updateAspectResults(List.of(
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name1"))
                            .auditStamp(new AuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name2"))
                            .auditStamp(new AuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name3"))
                            .auditStamp(new AuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name4"))
                            .auditStamp(new AuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name5"))
                            .auditStamp(new AuditStamp())
                            .request(req)
                            .build()))
            .build();
    when(aspectDao.runInTransactionWithRetry(any(), any(), anyInt())).thenReturn(Optional.of(txResult));
    aspectResource.ingestProposal(mcp, "false");
    verify(producer, times(5))
        .produceMetadataChangeLog(any(OperationContext.class), eq(urn), any(AspectSpec.class), any(MetadataChangeLog.class));
    verifyNoMoreInteractions(producer);
  }

  @Test
  public void testNoValidateAsync() throws URISyntaxException {
    OperationContext noValidateOpContext = TestOperationContexts.systemContextNoValidate();
    aspectResource.setSystemOperationContext(noValidateOpContext);
    reset(producer, aspectDao);
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    Urn urn = new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD);
    mcp.setEntityUrn(urn);
    GenericAspect properties = GenericRecordUtils.serializeAspect(new DatasetProperties().setName("name"));
    mcp.setAspect(GenericRecordUtils.serializeAspect(properties));
    mcp.setAspectName("notAnAspect");
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);
    aspectResource.ingestProposal(mcp, "true");
    verify(producer, times(1)).produceMetadataChangeProposal(any(OperationContext.class), eq(urn), argThat(arg -> arg.getMetadataChangeProposal().equals(mcp)));
    verifyNoMoreInteractions(producer);
    verifyNoMoreInteractions(aspectDao);
    reset(producer, aspectDao);
    aspectResource.setSystemOperationContext(opContext);
  }
}
