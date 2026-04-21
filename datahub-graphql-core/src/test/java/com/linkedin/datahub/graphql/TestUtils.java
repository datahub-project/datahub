package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;

public class TestUtils {

  public static EntityService<ChangeItemImpl> getMockEntityService() {
    return (EntityService<ChangeItemImpl>) Mockito.mock(EntityService.class);
  }

  public static QueryContext getMockAllowContext() {
    return getMockAllowContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockAllowContext(String actorUrn) {
    return getMockAllowContext(actorUrn, null);
  }

  public static QueryContext getMockAllowContext(String actorUrn, AuthorizationRequest request) {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);

    if (request == null) {
      // Simple case: always allow
      AuthorizationResult result =
          new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, "");
      when(mockAuthorizer.authorize(any())).thenReturn(result);
    } else {
      // Complex case: allow only for specific request
      when(mockAuthorizer.authorize(Mockito.any(AuthorizationRequest.class)))
          .thenAnswer(
              args -> {
                AuthorizationRequest req = args.getArgument(0);

                if (request.equals(req)) {
                  return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "");
                } else {
                  return new AuthorizationResult(req, AuthorizationResult.Type.DENY, "");
                }
              });
    }

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    when(mockContext.getAuthentication()).thenReturn(authentication);
    when(mockContext.getMaxParentDepth()).thenReturn(50);

    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  public static QueryContext getMockDenyContext() {
    return getMockDenyContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockDenyContext(String actorUrn) {
    return getMockDenyContext(actorUrn, null);
  }

  public static QueryContext getMockDenyContext(String actorUrn, AuthorizationRequest request) {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);

    Authorizer mockAuthorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    when(result.getType()).thenReturn(AuthorizationResult.Type.DENY);

    if (request == null) {
      // Simple case: always deny
      when(mockAuthorizer.authorize(any())).thenReturn(result);
    } else {
      // Specific case: deny only for this specific request
      when(mockAuthorizer.authorize(Mockito.eq(request))).thenReturn(result);
    }

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    when(mockContext.getAuthentication()).thenReturn(authentication);
    when(mockContext.getMaxParentDepth()).thenReturn(50);

    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  /**
   * Returns a deny {@link QueryContext} whose {@link OperationContext} has view authorization
   * enabled ({@code viewAuthorizationConfiguration.enabled=true}) and system authentication
   * disabled ({@code allowSystemAuthentication=false}).
   *
   * <p>This is necessary because the default {@link TestOperationContexts} contexts use {@code
   * enabled=false}, which causes {@code AuthorizationUtils.canView} to short-circuit to {@code
   * true} regardless of the authorizer's decision. Use this helper in tests that need {@code
   * canView} to actually enforce the deny decision.
   */
  public static QueryContext getMockDenyContextWithViewAuth() {
    return getMockDenyContextWithViewAuth("urn:li:corpuser:test");
  }

  public static QueryContext getMockDenyContextWithViewAuth(@Nonnull final String actorUrn) {
    Authorizer denyAuthorizer = mock(Authorizer.class);
    AuthorizationResult denyResult = mock(AuthorizationResult.class);
    when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(denyAuthorizer.authorize(any())).thenReturn(denyResult);

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    OperationContextConfig config =
        OperationContextConfig.builder()
            .viewAuthorizationConfiguration(
                ViewAuthorizationConfiguration.builder().enabled(true).build())
            .allowSystemAuthentication(false)
            .build();

    OperationContext operationContext =
        TestOperationContexts.Builder.builder()
            .configSupplier(() -> config)
            .buildSystemContext()
            .asSession(RequestContext.TEST, denyAuthorizer, authentication);

    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  public static void verifyIngestProposal(
      EntityService<?> mockService, int numberOfInvocations, MetadataChangeProposal proposal) {
    verifyIngestProposal(mockService, numberOfInvocations, List.of(proposal));
  }

  public static void verifyIngestProposal(
      EntityService<?> mockService,
      int numberOfInvocations,
      List<MetadataChangeProposal> proposals) {

    ArgumentCaptor<AspectsBatchImpl> batchCaptor = ArgumentCaptor.forClass(AspectsBatchImpl.class);

    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(any(), batchCaptor.capture(), Mockito.eq(false));

    // check has time
    Assert.assertTrue(
        batchCaptor.getValue().getItems().stream()
            .allMatch(prop -> prop.getSystemMetadata().getLastObserved() > 0L));

    // check without time
    Assert.assertEquals(
        batchCaptor.getValue().getItems().stream()
            .map(
                m -> {
                  m.getSystemMetadata().removeAspectModified();
                  m.getSystemMetadata().removeAspectCreated();
                  m.getSystemMetadata().removeSchemaVersion();
                  return m.getSystemMetadata().setLastObserved(0);
                })
            .collect(Collectors.toList()),
        proposals.stream()
            .map(
                m -> {
                  m.getSystemMetadata().removeAspectModified();
                  m.getSystemMetadata().removeAspectCreated();
                  m.getSystemMetadata().removeSchemaVersion();
                  return m.getSystemMetadata().setLastObserved(0);
                })
            .collect(Collectors.toList()));
  }

  public static void verifySingleIngestProposal(
      EntityService<?> mockService,
      int numberOfInvocations,
      MetadataChangeProposal expectedProposal) {
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(any(), proposalCaptor.capture(), any(AuditStamp.class), Mockito.eq(false));

    // check has time
    Assert.assertTrue(proposalCaptor.getValue().getSystemMetadata().getLastObserved() > 0L);

    // check without time
    proposalCaptor.getValue().getSystemMetadata().setLastObserved(0L);
    proposalCaptor.getValue().getSystemMetadata().removeSchemaVersion();
    expectedProposal.getSystemMetadata().setLastObserved(0L);
    expectedProposal.getSystemMetadata().removeSchemaVersion();
    Assert.assertEquals(proposalCaptor.getValue(), expectedProposal);
  }

  public static void verifyIngestProposal(EntityService<?> mockService, int numberOfInvocations) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(any(), any(AspectsBatchImpl.class), Mockito.eq(false));
  }

  public static void verifySingleIngestProposal(
      EntityService<?> mockService, int numberOfInvocations) {
    Mockito.verify(mockService, Mockito.times(numberOfInvocations))
        .ingestProposal(
            any(), any(MetadataChangeProposal.class), any(AuditStamp.class), Mockito.eq(false));
  }

  public static void verifyNoIngestProposal(EntityService<?> mockService) {
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  public static void verifyIngestProposal(
      EntityClient mockClient, int numberOfInvocations, MetadataChangeProposal expectedProposal)
      throws RemoteInvocationException {

    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);

    Mockito.verify(mockClient, Mockito.times(numberOfInvocations))
        .ingestProposal(any(), proposalCaptor.capture(), Mockito.eq(false));

    // check has time
    Assert.assertTrue(proposalCaptor.getValue().getSystemMetadata().getLastObserved() > 0L);

    // check without time
    proposalCaptor.getValue().getSystemMetadata().setLastObserved(0L);
    proposalCaptor.getValue().getSystemMetadata().removeSchemaVersion();
    expectedProposal.getSystemMetadata().setLastObserved(0L);
    expectedProposal.getSystemMetadata().removeSchemaVersion();
    Assert.assertEquals(proposalCaptor.getValue(), expectedProposal);
  }

  private TestUtils() {}
}
