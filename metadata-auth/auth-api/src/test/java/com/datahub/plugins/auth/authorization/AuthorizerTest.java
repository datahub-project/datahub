package com.datahub.plugins.auth.authorization;

import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationRequest;
import com.datahub.authorization.BatchAuthorizationResult;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class AuthorizerTest {
  @Test
  public void defaultAuthorizeBatchReturnsLazyResults() {
    var authorizeCallsCount = new AtomicInteger(0);
    Authorizer authorizer =
        new Authorizer() {
          @Override
          public AuthorizationResult authorize(@Nonnull AuthorizationRequest request) {
            return new AuthorizationResult(
                request,
                AuthorizationResult.Type.ALLOW,
                "%d".formatted(authorizeCallsCount.incrementAndGet()));
          }
        };

    BatchAuthorizationResult result =
        authorizer.authorizeBatch(
            new BatchAuthorizationRequest("urn:li:corpuser:test", Set.of("p1"), null, List.of()));

    assertEquals(authorizeCallsCount.get(), 0);

    assertNull(result.getResults().get("p2"));

    assertEquals(authorizeCallsCount.get(), 0);

    AuthorizationResult authorizationResult = result.getResults().get("p1");

    assertEquals(authorizationResult.getType(), AuthorizationResult.Type.ALLOW);
    assertEquals(authorizationResult.getMessage(), "1");
    assertEquals(authorizeCallsCount.get(), 1);

    AuthorizationResult authorizationResultAgain = result.getResults().get("p1");

    assertSame(authorizationResult, authorizationResultAgain);
    assertEquals(authorizeCallsCount.get(), 1);
  }
}
