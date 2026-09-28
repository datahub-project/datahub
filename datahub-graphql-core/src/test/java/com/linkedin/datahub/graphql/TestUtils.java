package com.linkedin.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.SessionActorIdentity;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.AuthorizationContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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
    return getMockAllowContext(actorUrn, (AuthorizationRequest) null);
  }

  public static QueryContext getMockAllowContext(
      @Nonnull String actorUrn, @Nonnull Collection<Urn> sessionGroupMembership) {
    return withSessionGroupMembership(getMockAllowContext(actorUrn), sessionGroupMembership);
  }

  /** Stubs session group membership on an existing mock context. */
  public static QueryContext withSessionGroupMembership(
      @Nonnull QueryContext context, @Nonnull Collection<Urn> sessionGroupMembership) {
    return withSessionActorIdentity(
        context,
        new SessionActorIdentity(
            UrnUtils.getUrn(context.getActorUrn()), List.copyOf(sessionGroupMembership), Set.of()));
  }

  /** Stubs session actor identity (corp + native groups) on an existing mock context. */
  public static QueryContext withSessionActorIdentity(
      @Nonnull QueryContext context, @Nonnull SessionActorIdentity sessionActorIdentity) {
    OperationContext operationContext = spy(context.getOperationContext());
    io.datahubproject.metadata.context.ActorContext actorContext =
        mock(io.datahubproject.metadata.context.ActorContext.class);
    when(actorContext.getActorUrn()).thenReturn(sessionActorIdentity.getActorUrn());
    when(actorContext.getGroupMembership()).thenReturn(sessionActorIdentity.getGroups());
    when(operationContext.getSessionActorContext()).thenReturn(actorContext);

    AuthorizationContext authorizationContext = mock(AuthorizationContext.class);
    when(authorizationContext.getSessionActorIdentity(sessionActorIdentity.getActorUrn()))
        .thenReturn(sessionActorIdentity);
    when(operationContext.getAuthorizationContext()).thenReturn(authorizationContext);

    when(context.getOperationContext()).thenReturn(operationContext);
    return context;
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
        withDefaultSearchEntityTypes(
            TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication));
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  /**
   * Enriches an {@link OperationContext} with YAML-default entity-type lists on {@link
   * SearchContext} so GraphQL resolvers that fall back to configured defaults behave like
   * production in unit tests.
   */
  public static OperationContext withDefaultSearchEntityTypes(
      @Nonnull OperationContext operationContext) {
    SearchContext enriched =
        operationContext.getSearchContext().toBuilder()
            .defaultSearchEntityNames(
                EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_SEARCH_ENTITY_TYPES))
            .defaultAutocompleteEntityNames(
                EntityTypeListConfig.parseCsv(
                    EntityTypeListConfig.DEFAULT_AUTOCOMPLETE_ENTITY_TYPES))
            .defaultBrowseEntityNames(
                EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_BROWSE_ENTITY_TYPES))
            .prioritizedSourceEntityTypes(
                EntityTypeListConfig.parseCsv(
                    EntityTypeListConfig.DEFAULT_PRIORITIZED_SOURCE_ENTITY_TYPES))
            .prioritizedDatahubEntityTypes(
                EntityTypeListConfig.parseCsv(
                    EntityTypeListConfig.DEFAULT_PRIORITIZED_DATAHUB_ENTITY_TYPES))
            .build();
    return operationContext.toBuilder()
        .searchContext(enriched)
        .build(operationContext.getSessionActorContext(), false);
  }

  /**
   * Returns a context that allows a single privilege on a single resource URN and denies everything
   * else. Lets a test assert which URN a check was actually made against, rather than only whether
   * it passed.
   */
  public static QueryContext getMockAllowContextForResource(
      @Nonnull final String actorUrn,
      @Nonnull final String privilege,
      @Nonnull final Urn allowedResourceUrn) {
    Authorizer mockAuthorizer = mock(Authorizer.class);
    when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            args -> {
              AuthorizationRequest request = args.getArgument(0);
              boolean allowed =
                  privilege.equals(request.getPrivilege())
                      && request
                          .getResourceSpec()
                          .map(spec -> allowedResourceUrn.toString().equals(spec.getEntity()))
                          .orElse(false);
              return new AuthorizationResult(
                  request,
                  allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                  "");
            });

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);
    when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    when(mockContext.getAuthentication()).thenReturn(authentication);
    OperationContext operationContext =
        withDefaultSearchEntityTypes(
            TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication));
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
        withDefaultSearchEntityTypes(
            TestOperationContexts.userContextNoSearchAuthorization(mockAuthorizer, authentication));
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  /**
   * Returns a deny {@link QueryContext} backed by a real {@link OperationContext} so authorization
   * checks that route through {@code OperationContext.authorize(...)} (e.g. {@code
   * AuthUtil.canViewEntity}) actually enforce the deny decision.
   *
   * <p>The plain {@link #getMockDenyContext()} only mocks {@code QueryContext.getAuthorizer()} and
   * leaves {@code getOperationContext()} unset; that works for checks that resolve via {@code
   * QueryContext.getAuthorizer()} but NPEs for checks that go through {@code OperationContext}.
   */
  public static QueryContext getMockDenyContextWithOperationContext() {
    return getMockDenyContextWithOperationContext("urn:li:corpuser:test");
  }

  public static QueryContext getMockDenyContextWithOperationContext(
      @Nonnull final String actorUrn) {
    Authorizer denyAuthorizer = mock(Authorizer.class);
    AuthorizationResult denyResult = mock(AuthorizationResult.class);
    when(denyResult.getType()).thenReturn(AuthorizationResult.Type.DENY);
    when(denyAuthorizer.authorize(any())).thenReturn(denyResult);

    Authentication authentication =
        new Authentication(new Actor(ActorType.USER, UrnUtils.getUrn(actorUrn).getId()), "creds");

    OperationContext operationContext =
        TestOperationContexts.userContextNoSearchAuthorization(denyAuthorizer, authentication);

    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getActorUrn()).thenReturn(actorUrn);
    when(mockContext.getOperationContext()).thenReturn(operationContext);

    return mockContext;
  }

  /**
   * Stubs batched existence resolution so that exactly the given urns are reported as existing.
   * Batch mutations resolve existence for a whole group of urns in one call, so the stub answers
   * with the requested urns intersected against {@code existing} rather than a fixed set.
   */
  public static void stubExistingUrns(EntityService<?> mockService, Urn... existing) {
    final Set<Urn> existingUrns = Set.of(existing);
    when(mockService.exists(any(), anyCollection(), eq(true)))
        .thenAnswer(
            invocation -> {
              final Collection<Urn> requested = invocation.getArgument(1);
              return requested.stream().filter(existingUrns::contains).collect(Collectors.toSet());
            });
  }

  /**
   * Asserts that existence was resolved in {@code expectedBatchCalls} batched calls and never one
   * urn at a time. Use this where the number of groups is the point of the test; prefer {@link
   * #verifyExistenceResolvedInBatches(EntityService)} elsewhere, so tests do not pin down a call
   * count they are not actually asserting anything about.
   */
  public static void verifyExistenceResolvedInBatches(
      EntityService<?> mockService, int expectedBatchCalls) {
    Mockito.verify(mockService, Mockito.times(expectedBatchCalls))
        .exists(any(), anyCollection(), eq(true));
    verifyExistenceResolvedInBatches(mockService);
  }

  /** Asserts that existence was resolved via the batched call and never one urn at a time. */
  public static void verifyExistenceResolvedInBatches(EntityService<?> mockService) {
    Mockito.verify(mockService, Mockito.atLeastOnce()).exists(any(), anyCollection(), eq(true));
    Mockito.verify(mockService, Mockito.never())
        .exists(any(), any(Urn.class), Mockito.anyBoolean());
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
