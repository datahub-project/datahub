package com.linkedin.datahub.graphql;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.Mockito;


public class TestUtils {

  public static QueryContext getMockAllowContext() {
    return getMockAllowContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockAllowContext(String actorUrn) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds")
    );
    return mockContext;
  }

  public static QueryContext getMockDenyContext() {
    return getMockDenyContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockDenyContext(String actorUrn) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    AuthorizationResult result = Mockito.mock(AuthorizationResult.class);
    Mockito.when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(result);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds")
    );
    return mockContext;
  }

  public static void verifyIngestProposal(EntityService mockService, int numberOfInvocations, MetadataChangeProposal proposal) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations)).ingestProposal(
        Mockito.eq(proposal),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  public static void verifyIngestProposal(EntityService mockService, int numberOfInvocations) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
  }

  public static void verifyNoIngestProposal(EntityService mockService) {
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(),
        Mockito.any(AuditStamp.class), Mockito.anyBoolean());
  }

  private TestUtils() { }
}
