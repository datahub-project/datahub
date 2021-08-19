package com.linkedin.metadata.filter.auth;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendServerAuthenticator implements Authenticator {
  @Override
  public AuthenticationResult authenticate(AuthenticationContext ctx) {
    log.info("Looking for principal in headers");
    // Frontend server provides the principal. Just use that.
    if (ctx.requestHeaders().containsKey("actor")) {
      final String principal = ctx.requestHeaders().get("actor");
      log.info(String.format("Actor is %s", principal));
      return new AuthenticationResultImpl(new PrincipalImpl(principal, Collections.emptyMap()));
    }
    log.warn("Creating a fake actor because none was found in the headers.");
    final String actor = "system";
    return new AuthenticationResultImpl(new PrincipalImpl(actor, Collections.emptyMap()));
  }
}
