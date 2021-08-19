package com.linkedin.metadata.filter.auth;

import com.linkedin.restli.server.filter.FilterRequestContext;
import java.util.Map;


public class RestliAuthenticationContext implements AuthenticationContext {

  private final FilterRequestContext restliContext;

  public RestliAuthenticationContext(final FilterRequestContext restliContext) {
    this.restliContext = restliContext;
  }

  @Override
  public Map<String, String> requestHeaders() {
    return restliContext.getRequestHeaders();
  }
}
