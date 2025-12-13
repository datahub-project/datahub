package com.datahub.authorization;

import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AuthorizerChainTest {
  @Test
  public void testMultipleLazyAuthorizers() {
    var actorUrn = "actorUrn";
    var privilege1 = "privilege1";
    var privilege2 = "privilege2";
    Authorizer authorizer1 = Mockito.mock(Authorizer.class, Mockito.CALLS_REAL_METHODS);
    Authorizer authorizer2 = Mockito.mock(Authorizer.class, Mockito.CALLS_REAL_METHODS);
    AuthorizationRequest request1 =
        new AuthorizationRequest(actorUrn, privilege1, Optional.empty(), List.of());
    AuthorizationRequest request2 =
        new AuthorizationRequest(actorUrn, privilege2, Optional.empty(), List.of());
    Mockito.when(authorizer1.authorize(request1))
        .thenReturn(
            new AuthorizationResult(request1, AuthorizationResult.Type.ALLOW, "from authorizer1"));
    Mockito.when(authorizer1.authorize(request2))
        .thenReturn(
            new AuthorizationResult(request2, AuthorizationResult.Type.DENY, "from authorizer1"));
    Mockito.when(authorizer2.authorize(request1))
        .thenReturn(
            new AuthorizationResult(request1, AuthorizationResult.Type.DENY, "from authorizer2"));
    Mockito.when(authorizer2.authorize(request2))
        .thenReturn(
            new AuthorizationResult(request2, AuthorizationResult.Type.ALLOW, "from authorizer2"));
    AuthorizerChain authorizerChain = new AuthorizerChain(List.of(authorizer1, authorizer2), null);
    BatchAuthorizationResult result =
        authorizerChain.authorizeBatch(
            new BatchAuthorizationRequest(
                actorUrn, Set.of(privilege1, privilege2), Optional.empty(), List.of()));

    Assert.assertNotNull(result);
    Assert.assertEquals(
        result.getResults().get(privilege1).getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(
        result.getResults().get(privilege2).getType(), AuthorizationResult.Type.ALLOW);

    Mockito.verify(authorizer1, Mockito.times(2)).authorize(ArgumentMatchers.any());
    Mockito.verify(authorizer2, Mockito.times(1)).authorize(ArgumentMatchers.any());
  }

  @Test
  public void testOneAuthorizerReturnsError() {
    var actorUrn = "actorUrn";
    var privilege1 = "privilege1";
    var privilege2 = "privilege2";
    Authorizer authorizer1 = Mockito.mock(Authorizer.class, Mockito.CALLS_REAL_METHODS);
    Authorizer authorizer2 = Mockito.mock(Authorizer.class);
    AuthorizationRequest request1 =
        new AuthorizationRequest(actorUrn, privilege1, Optional.empty(), List.of());
    AuthorizationRequest request2 =
        new AuthorizationRequest(actorUrn, privilege2, Optional.empty(), List.of());
    Mockito.when(authorizer1.authorize(request1))
        .thenReturn(
            new AuthorizationResult(request1, AuthorizationResult.Type.ALLOW, "from authorizer1"));
    Mockito.when(authorizer1.authorize(request2))
        .thenReturn(
            new AuthorizationResult(request2, AuthorizationResult.Type.DENY, "from authorizer1"));
    Mockito.when(authorizer2.authorizeBatch(ArgumentMatchers.any()))
        .thenThrow(new IllegalArgumentException("failed to execute authorization logic"));
    AuthorizerChain authorizerChain = new AuthorizerChain(List.of(authorizer1, authorizer2), null);
    BatchAuthorizationResult result =
        authorizerChain.authorizeBatch(
            new BatchAuthorizationRequest(
                actorUrn, Set.of(privilege1, privilege2), Optional.empty(), List.of()));

    Assert.assertNotNull(result);
    Assert.assertEquals(
        result.getResults().get(privilege1).getType(), AuthorizationResult.Type.ALLOW);
    Assert.assertEquals(
        result.getResults().get(privilege2).getType(), AuthorizationResult.Type.DENY);

    Mockito.verify(authorizer1, Mockito.times(2)).authorize(ArgumentMatchers.any());
    Mockito.verify(authorizer2, Mockito.times(0)).authorize(ArgumentMatchers.any());
    Mockito.verify(authorizer2, Mockito.times(1)).authorizeBatch(ArgumentMatchers.any());
  }
}
