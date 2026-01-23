package io.datahubproject.metadata.context;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AuthorizationContextTest {
  @Test
  public void testCaching() {
    Map<String, AuthorizationResult> results = Mockito.mock(Map.class);
    Mockito.when(results.get(ArgumentMatchers.eq("privilege1")))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));

    var authorizer = Mockito.mock(Authorizer.class);
    Mockito.when(authorizer.authorizeBatch(ArgumentMatchers.any()))
        .thenReturn(new BatchAuthorizationResult(null, results));
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();

    BatchAuthorizationResult result1 =
        authorizationContext.authorize(
            ActorContext.builder()
                .authentication(new Authentication(new Actor(ActorType.USER, "user"), "creds"))
                .build(),
            Set.of("privilege1"),
            null,
            List.of());

    BatchAuthorizationResult result2 =
        authorizationContext.authorize(
            ActorContext.builder()
                .authentication(new Authentication(new Actor(ActorType.USER, "user"), "creds"))
                .build(),
            Set.of("privilege1"),
            null,
            List.of());

    Assert.assertEquals(result1, result2);
    Assert.assertSame(
        result1.getResults().get("privilege1"), result2.getResults().get("privilege1"));

    Mockito.verify(authorizer, Mockito.times(2)).authorizeBatch(ArgumentMatchers.any());
    Mockito.verify(results).get("privilege1");
  }
}
