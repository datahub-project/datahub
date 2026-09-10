package com.datahub.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.auth.authorization.ResourceSpecCachingAuthorizer;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextAuthorizer;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AuthorizerChainTest {

  private static final AuthorizationRequest REQUEST =
      new AuthorizationRequest(
          "urn:li:corpuser:testuser", "VIEW", Optional.empty(), Collections.emptyList());

  private DataHubAuthorizer defaultAuthorizer;
  private OperationContext opContext;

  @BeforeMethod
  public void setUp() {
    defaultAuthorizer = mock(DataHubAuthorizer.class);
    opContext = TestOperationContexts.systemContextNoValidate();
  }

  @Test
  public void authorizeWithOperationContext_delegatesToOperationContextAuthorizerOnAllow() {
    Authorizer delegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) delegate;
    when(operationContextAuthorizer.authorize(any(), any(), eq(opContext)))
        .thenReturn(new AuthorizationResult(REQUEST, AuthorizationResult.Type.ALLOW, null));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    AuthorizationResult result = chain.authorize(REQUEST, null, opContext);

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(operationContextAuthorizer, times(1)).authorize(any(), any(), eq(opContext));
    verify(delegate, never()).authorize(any(AuthorizationRequest.class));
  }

  @Test
  public void authorizeWithOperationContext_delegatesToResourceSpecCachingAuthorizer() {
    Authorizer delegate =
        mock(Authorizer.class, withSettings().extraInterfaces(ResourceSpecCachingAuthorizer.class));
    ResourceSpecCachingAuthorizer cachingAuthorizer = (ResourceSpecCachingAuthorizer) delegate;
    Map<EntitySpec, ResolvedEntitySpec> cache = Map.of();
    when(cachingAuthorizer.authorize(any(), eq(cache)))
        .thenReturn(new AuthorizationResult(REQUEST, AuthorizationResult.Type.ALLOW, null));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    AuthorizationResult result = chain.authorize(REQUEST, cache, opContext);

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(cachingAuthorizer, times(1)).authorize(any(), eq(cache));
    verify(delegate, never()).authorize(any(AuthorizationRequest.class));
  }

  @Test
  public void authorizeWithOperationContext_delegatesToPlainAuthorizer() {
    Authorizer delegate = mock(Authorizer.class);
    when(delegate.authorize(any()))
        .thenReturn(new AuthorizationResult(REQUEST, AuthorizationResult.Type.ALLOW, null));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    AuthorizationResult result = chain.authorize(REQUEST, null, opContext);

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(delegate, times(1)).authorize(any(AuthorizationRequest.class));
  }

  @Test
  public void authorizeWithOperationContext_returnsDenyWhenAllDelegatesDeny() {
    Authorizer delegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) delegate;
    when(operationContextAuthorizer.authorize(any(), any(), eq(opContext)))
        .thenReturn(new AuthorizationResult(REQUEST, AuthorizationResult.Type.DENY, "denied"));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    AuthorizationResult result = chain.authorize(REQUEST, null, opContext);

    assertEquals(result.getType(), AuthorizationResult.Type.DENY);
  }

  @Test
  public void authorizeWithOperationContext_skipsAuthorizerOnException() {
    Authorizer failingDelegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer failingOperationContextAuthorizer =
        (OperationContextAuthorizer) failingDelegate;
    when(failingOperationContextAuthorizer.authorize(any(), any(), eq(opContext)))
        .thenThrow(new RuntimeException("boom"));

    Authorizer succeedingDelegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer succeedingOperationContextAuthorizer =
        (OperationContextAuthorizer) succeedingDelegate;
    when(succeedingOperationContextAuthorizer.authorize(any(), any(), eq(opContext)))
        .thenReturn(new AuthorizationResult(REQUEST, AuthorizationResult.Type.ALLOW, null));

    AuthorizerChain chain =
        new AuthorizerChain(List.of(failingDelegate, succeedingDelegate), defaultAuthorizer);

    AuthorizationResult result = chain.authorize(REQUEST, null, opContext);

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(succeedingOperationContextAuthorizer, times(1)).authorize(any(), any(), eq(opContext));
  }

  @Test
  public void
      resolveSessionActorIdentityWithOperationContext_delegatesToOperationContextAuthorizer() {
    Authorizer delegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) delegate;
    SessionActorIdentity identity =
        SessionActorIdentity.empty(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    when(operationContextAuthorizer.resolveSessionActorIdentity(
            eq(identity.getActorUrn()), eq(opContext)))
        .thenReturn(Optional.of(identity));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    Optional<SessionActorIdentity> result =
        chain.resolveSessionActorIdentity(identity.getActorUrn(), opContext);

    assertTrue(result.isPresent());
    assertEquals(result.get(), identity);
  }

  @Test
  public void resolveSessionActorIdentityWithOperationContext_ignoresPlainAuthorizers() {
    Authorizer delegate = mock(Authorizer.class);
    when(delegate.resolveSessionActorIdentity(any()))
        .thenReturn(Optional.of(SessionActorIdentity.empty(UrnUtils.getUrn("urn:li:corpuser:x"))));

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    Optional<SessionActorIdentity> result =
        chain.resolveSessionActorIdentity(UrnUtils.getUrn("urn:li:corpuser:testuser"), opContext);

    assertFalse(result.isPresent());
    verify(delegate, never()).resolveSessionActorIdentity(any());
  }

  @Test
  public void getActorPoliciesWithOperationContext_delegatesToOperationContextAuthorizer() {
    Authorizer delegate =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) delegate;
    SessionActorIdentity identity =
        SessionActorIdentity.empty(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    Set<DataHubPolicyInfo> policies = Set.of(new DataHubPolicyInfo());
    when(operationContextAuthorizer.getActorPolicies(
            eq(identity.getActorUrn()), eq(identity), isNull(), isNull(), eq(opContext)))
        .thenReturn(policies);

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    Set<DataHubPolicyInfo> result =
        chain.getActorPolicies(identity.getActorUrn(), identity, null, null, opContext);

    assertEquals(result, policies);
  }

  @Test
  public void getActorGroupsWithOperationContext_delegatesToDataHubAuthorizer() {
    DataHubAuthorizer delegate = mock(DataHubAuthorizer.class);
    List<com.linkedin.common.urn.Urn> groups = List.of(UrnUtils.getUrn("urn:li:corpGroup:test"));
    when(delegate.getActorGroups(any(), eq(opContext))).thenReturn(groups);

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    assertEquals(
        chain.getActorGroups(UrnUtils.getUrn("urn:li:corpuser:testuser"), opContext), groups);
  }

  @Test
  public void getActorGroupsWithOperationContext_fallsBackToPlainGetActorGroups() {
    Authorizer delegate = mock(Authorizer.class);
    List<com.linkedin.common.urn.Urn> groups = List.of(UrnUtils.getUrn("urn:li:corpGroup:test"));
    when(delegate.getActorGroups(any())).thenReturn(groups);

    AuthorizerChain chain = new AuthorizerChain(List.of(delegate), defaultAuthorizer);

    assertEquals(
        chain.getActorGroups(UrnUtils.getUrn("urn:li:corpuser:testuser"), opContext), groups);
    verify(delegate, times(1)).getActorGroups(any());
  }
}
