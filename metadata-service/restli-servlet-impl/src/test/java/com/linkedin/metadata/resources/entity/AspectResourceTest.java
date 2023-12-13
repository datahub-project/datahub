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
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.transactions.UpsertBatchItem;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.UpdateIndicesService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.List;
import mock.MockEntityRegistry;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class AspectResourceTest {
  private AspectResource _aspectResource;
  private EntityService _entityService;
  private AspectDao _aspectDao;
  private EventProducer _producer;
  private EntityRegistry _entityRegistry;
  private UpdateIndicesService _updateIndicesService;
  private PreProcessHooks _preProcessHooks;
  private Authorizer _authorizer;

  @BeforeTest
  public void setup() {
    _aspectResource = new AspectResource();
    _aspectDao = mock(AspectDao.class);
    _producer = mock(EventProducer.class);
    _entityRegistry = new MockEntityRegistry();
    _updateIndicesService = mock(UpdateIndicesService.class);
    _preProcessHooks = mock(PreProcessHooks.class);
    _entityService =
        new EntityServiceImpl(
            _aspectDao, _producer, _entityRegistry, false, _updateIndicesService, _preProcessHooks);
    _authorizer = mock(Authorizer.class);
    _aspectResource.setAuthorizer(_authorizer);
    _aspectResource.setEntityService(_entityService);
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

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);
    _aspectResource.ingestProposal(mcp, "true");
    verify(_producer, times(1)).produceMetadataChangeProposal(urn, mcp);
    verifyNoMoreInteractions(_producer);
    verifyNoMoreInteractions(_aspectDao);

    reset(_producer, _aspectDao);

    UpsertBatchItem req =
        UpsertBatchItem.builder()
            .urn(urn)
            .aspectName(mcp.getAspectName())
            .aspect(mcp.getAspect())
            .metadataChangeProposal(mcp)
            .build(_entityRegistry);
    when(_aspectDao.runInTransactionWithRetry(any(), any(), anyInt()))
        .thenReturn(
            List.of(
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
                    .build()));
    _aspectResource.ingestProposal(mcp, "false");
    verify(_producer, times(5))
        .produceMetadataChangeLog(eq(urn), any(AspectSpec.class), any(MetadataChangeLog.class));
    verifyNoMoreInteractions(_producer);
  }
}
