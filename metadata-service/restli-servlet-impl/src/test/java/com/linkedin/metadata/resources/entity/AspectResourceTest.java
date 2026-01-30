package com.linkedin.metadata.resources.entity;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.domain.Domains;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.linkedin.mxe.SystemMetadata;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
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
    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });
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
            .auditStamp(opContext.getAuditStamp())
            .metadataChangeProposal(mcp)
            .build(opContext.getAspectRetriever());
    IngestAspectsResult txResult = IngestAspectsResult.builder()
            .updateAspectResults(List.of(
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name1"))
                            .auditStamp(opContext.getAuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name2"))
                            .auditStamp(opContext.getAuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name3"))
                            .auditStamp(opContext.getAuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name4"))
                            .auditStamp(opContext.getAuditStamp())
                            .request(req)
                            .build(),
                    UpdateAspectResult.builder()
                            .urn(urn)
                            .newValue(new DatasetProperties().setName("name5"))
                            .auditStamp(opContext.getAuditStamp())
                            .request(req)
                            .build()))
            .build();
    when(aspectDao.runInTransactionWithRetry(any(), any(), anyInt())).thenReturn(Optional.of(txResult));
    aspectResource.ingestProposal(mcp, "false");
    verify(producer, times(5))
        .produceMetadataChangeLog(any(OperationContext.class), eq(urn), any(AspectSpec.class), any(MetadataChangeLog.class));
    verifyNoMoreInteractions(producer);
  }

  @Test(expectedExceptions = RestLiServiceException.class, expectedExceptionsMessageRegExp = "Unknown aspect notAnAspect for entity dataset")
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
  }

  @Test
  public void testIngestProposalWithDomain_Authorized() throws URISyntaxException {
    reset(producer, aspectDao, authorizer);

    Urn datasetUrn = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.PROD);
    Urn domainUrn = Urn.createFromString("urn:li:domain:finance");

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setEntityUrn(datasetUrn);
    mcp.setAspectName(DOMAINS_ASPECT_NAME);
    Domains domains = new Domains().setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    mcp.setAspect(GenericRecordUtils.serializeAspect(domains));
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    aspectResource.ingestProposal(mcp, "true");

    verify(producer, times(1)).produceMetadataChangeProposal(
        any(OperationContext.class), eq(datasetUrn), any());
    verify(authorizer, atLeastOnce()).authorize(any(AuthorizationRequest.class));
  }

  @Test(expectedExceptions = RestLiServiceException.class)
  public void testIngestProposalWithDomain_Unauthorized() throws Throwable {
    reset(producer, aspectDao, authorizer);

    Urn datasetUrn = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.PROD);
    Urn domainUrn = Urn.createFromString("urn:li:domain:finance");

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setEntityUrn(datasetUrn);
    mcp.setAspectName(DOMAINS_ASPECT_NAME);
    Domains domains = new Domains().setDomains(new UrnArray(Collections.singletonList(domainUrn)));
    mcp.setAspect(GenericRecordUtils.serializeAspect(domains));
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Unauthorized - missing domain access");
    });

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    try {
      aspectResource.ingestProposal(mcp, "true").get();
    } catch (Exception e) {
      verify(producer, never()).produceMetadataChangeProposal(any(), any(), any());
      throw e.getCause() != null ? e.getCause() : e;
    }
  }

  @Test(expectedExceptions = RestLiServiceException.class)
  public void testIngestProposalBatchWithMixedAuthorization() throws Throwable {
    reset(producer, aspectDao, authorizer);

    Urn dataset1Urn = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.PROD);
    Urn dataset2Urn = new DatasetUrn(new DataPlatformUrn("platform"), "dataset2", FabricType.PROD);
    Urn financeDomainUrn = Urn.createFromString("urn:li:domain:finance");
    Urn marketingDomainUrn = Urn.createFromString("urn:li:domain:marketing");

    MetadataChangeProposal mcp1 = new MetadataChangeProposal();
    mcp1.setEntityType(DATASET_ENTITY_NAME);
    mcp1.setEntityUrn(dataset1Urn);
    mcp1.setAspectName(DOMAINS_ASPECT_NAME);
    Domains domains1 = new Domains().setDomains(new UrnArray(Collections.singletonList(financeDomainUrn)));
    mcp1.setAspect(GenericRecordUtils.serializeAspect(domains1));
    mcp1.setChangeType(ChangeType.UPSERT);
    mcp1.setSystemMetadata(new SystemMetadata());

    MetadataChangeProposal mcp2 = new MetadataChangeProposal();
    mcp2.setEntityType(DATASET_ENTITY_NAME);
    mcp2.setEntityUrn(dataset2Urn);
    mcp2.setAspectName(DOMAINS_ASPECT_NAME);
    Domains domains2 = new Domains().setDomains(new UrnArray(Collections.singletonList(marketingDomainUrn)));
    mcp2.setAspect(GenericRecordUtils.serializeAspect(domains2));
    mcp2.setChangeType(ChangeType.UPSERT);
    mcp2.setSystemMetadata(new SystemMetadata());

    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      // Check if the resource being authorized is dataset2 (marketing domain)
      if (request.getResourceSpec().isPresent() &&
          request.getResourceSpec().get().getEntity().equals(dataset2Urn.toString())) {
        return new AuthorizationResult(request, AuthorizationResult.Type.DENY, "Unauthorized - no access to dataset2");
      }
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    try {
      aspectResource.ingestProposalBatch(new MetadataChangeProposal[]{mcp1, mcp2}, "true").get();
    } catch (Exception e) {
      verify(producer, never()).produceMetadataChangeProposal(any(), any(), any());
      throw e.getCause() != null ? e.getCause() : e;
    }
  }

  @Test
  public void testIngestProposalWithoutDomain() throws URISyntaxException {
    reset(producer, aspectDao, authorizer);

    Urn datasetUrn = new DatasetUrn(new DataPlatformUrn("platform"), "dataset1", FabricType.PROD);

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(DATASET_ENTITY_NAME);
    mcp.setEntityUrn(datasetUrn);
    mcp.setAspectName(DATASET_PROPERTIES_ASPECT_NAME);
    DatasetProperties properties = new DatasetProperties().setName("dataset1");
    mcp.setAspect(GenericRecordUtils.serializeAspect(properties));
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    when(authorizer.authorize(any(AuthorizationRequest.class))).thenAnswer(invocation -> {
      AuthorizationRequest request = invocation.getArgument(0);
      return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
    });

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    aspectResource.ingestProposal(mcp, "true");

    verify(producer, times(1)).produceMetadataChangeProposal(
        any(OperationContext.class), eq(datasetUrn), any());
  }

  @Test
  public void testIngestProposalSystemEntity_BypassesAuthorization() throws URISyntaxException {
    reset(producer, aspectDao, authorizer);

    Urn executionRequestUrn = Urn.createFromString("urn:li:dataHubExecutionRequest:test-id");

    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityType(EXECUTION_REQUEST_ENTITY_NAME);
    mcp.setEntityUrn(executionRequestUrn);
    mcp.setAspectName(EXECUTION_REQUEST_INPUT_ASPECT_NAME);

    // Create a valid ExecutionRequestInput with all required fields
    ExecutionRequestInput input = new ExecutionRequestInput();
    input.setTask("TEST_TASK");
    input.setArgs(new StringMap());
    input.setExecutorId("test-executor");
    input.setSource(new ExecutionRequestSource().setType("MANUAL_EXECUTION_REQUEST"));
    input.setRequestedAt(System.currentTimeMillis());

    mcp.setAspect(GenericRecordUtils.serializeAspect(input));
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setSystemMetadata(new SystemMetadata());

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    aspectResource.ingestProposal(mcp, "true");

    verify(producer, times(1)).produceMetadataChangeProposal(
        any(OperationContext.class), eq(executionRequestUrn), any());
    verify(authorizer, never()).authorize(any(AuthorizationRequest.class));
  }
}
