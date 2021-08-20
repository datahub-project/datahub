package com.linkedin.metadata.filter.auth;

import com.linkedin.restli.server.filter.Filter;
import com.linkedin.restli.server.filter.FilterRequestContext;
import com.linkedin.restli.server.filter.FilterResponseContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthenticationFilter implements Filter {

  private static final ThreadLocal<Principal> principal = new ThreadLocal<>();
  private final List<Authenticator> authenticators;

  public AuthenticationFilter(final List<Authenticator> authenticators) {
    this.authenticators = authenticators;
  }

  @Override
  public CompletableFuture<Void> onRequest(final FilterRequestContext requestContext) {

    final AuthenticationContext ctx = new RestliAuthenticationContext(requestContext);

    AuthenticationResult result = null;
    for (Authenticator authenticator : authenticators) {
      try {
        result = authenticator.authenticate(ctx);
        // Auth was successful. Exit loop optimistically.
        break;
      } catch (Exception e) {
        log.error("Failed to authenticate incoming request. Trying next authenticator..");
      }
    }

    if (result != null) {
      // There is a logged in user, so log the identity to the thread local context.
      principal.set(result.principal());
      return CompletableFuture.completedFuture(null);
    } else {
      // In reality, should throw 401.
      throw new RuntimeException("Failed to authenticate user");
    }
  }

  @Override
  public CompletableFuture<Void> onResponse(
      final FilterRequestContext requestContext,
      final FilterResponseContext responseContext) {
    principal.remove();
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> onError(
      Throwable th,
      final FilterRequestContext requestContext,
      final FilterResponseContext responseContext) {
    principal.remove();
    return CompletableFuture.completedFuture(null);
  }
}