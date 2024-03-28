package com.linkedin.datahub.graphql;

import static org.mockito.Mockito.mock;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.Mockito;

public class TestUtils {

  public static EntityService<ChangeItemImpl> getMockEntityService() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    EntityRegistry registry =
        new ConfigEntityRegistry(TestUtils.class.getResourceAsStream("/test-entity-registry.yaml"));
    EntityService<ChangeItemImpl> mockEntityService =
        (EntityService<ChangeItemImpl>) Mockito.mock(EntityService.class);
    Mockito.when(mockEntityService.getEntityRegistry()).thenReturn(registry);
    return mockEntityService;
  }

  public static QueryContext getMockAllowContext() {
    return getMockAllowContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockAllowContext(String actorUrn) {
    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");
    Mockito.when(mockContext.getAuthentication()).thenReturn(authentication);
    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication);
    Mockito.when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  public static QueryContext getMockAllowContext(String actorUrn, AuthorizationRequest request) {
    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication())
        .thenReturn(
            new Authentication(
                new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds"));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    return mockContext;
  }

  public static QueryContext getMockDenyContext() {
    return getMockDenyContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockDenyContext(String actorUrn) {
    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication())
        .thenReturn(
            new Authentication(
                new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds"));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    return mockContext;
  }

  public static QueryContext getMockDenyContext(String actorUrn, AuthorizationRequest request) {
    QueryContext mockContext = mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication())
        .thenReturn(
            new Authentication(
                new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds"));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(Mockito.mock(OperationContext.class));
    return mockContext;
  }

  public static void verifyIngestProposal(
      EntityService<ChangeItemImpl> mockService,
      int numberOfInvocations,
      MetadataChangeProposal proposal) {
    verifyIngestProposal(mockService, numberOfInvocations, List.of(proposal));
  }

  public static void verifyIngestProposal(
      EntityService<ChangeItemImpl> mockService,
      int numberOfInvocations,
      List<MetadataChangeProposal> proposals) {
    AspectsBatchImpl batch =
        AspectsBatchImpl.builder().mcps(proposals, mock(AuditStamp.class), mockService).build();
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(Mockito.eq(batch), Mockito.eq(false));
  }

  public static void verifySingleIngestProposal(
      EntityService<ChangeItemImpl> mockService,
      int numberOfInvocations,
      MetadataChangeProposal proposal) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(AuditStamp.class), Mockito.eq(false));
  }

  public static void verifyIngestProposal(
      EntityService<ChangeItemImpl> mockService, int numberOfInvocations) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(Mockito.any(AspectsBatchImpl.class), Mockito.eq(false));
  }

  public static void verifySingleIngestProposal(
      EntityService<ChangeItemImpl> mockService, int numberOfInvocations) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  public static void verifyNoIngestProposal(EntityService<ChangeItemImpl> mockService) {
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  private TestUtils() {}
}
