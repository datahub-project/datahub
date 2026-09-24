package io.datahubproject.openapi.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;

/** Shared stubs for mocked {@link AuthorizerChain} beans in openapi-servlet integration tests. */
public final class AuthorizerChainTestSupport {

  private static final AuthorizationResult ALLOW =
      new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, "");

  private AuthorizerChainTestSupport() {}

  /**
   * Stubs only the 1-arg {@link AuthorizerChain#authorize(AuthorizationRequest)} path. Exercises
   * {@link io.datahubproject.metadata.context.AuthorizationContext}'s fallback when a mock {@link
   * io.datahubproject.metadata.context.OperationContextAuthorizer} returns null.
   */
  public static void stubAllowViaOneArgOnly(AuthorizerChain authorizerChain) {
    when(authorizerChain.authorize(any(AuthorizationRequest.class))).thenReturn(ALLOW);
  }

  /**
   * Stubs the session {@link io.datahubproject.metadata.context.OperationContextAuthorizer} 3-arg
   * path used by {@link io.datahubproject.metadata.context.AuthorizationContext}.
   */
  public static void stubAllowViaOperationContextAuthorizer(AuthorizerChain authorizerChain) {
    when(authorizerChain.authorize(
            any(AuthorizationRequest.class), any(Map.class), any(OperationContext.class)))
        .thenReturn(ALLOW);
  }

  /** Stubs both authorization entry points. */
  public static void stubAllowAllPaths(AuthorizerChain authorizerChain) {
    stubAllowViaOneArgOnly(authorizerChain);
    stubAllowViaOperationContextAuthorizer(authorizerChain);
  }
}
