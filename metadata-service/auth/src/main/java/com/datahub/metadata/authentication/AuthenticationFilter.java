package com.datahub.metadata.authentication;

import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.AuthenticatorChain;
import com.datahub.authentication.authenticators.DataHubFrontendAuthenticator;
import com.datahub.authentication.authenticators.DataHubTokenAuthenticator;
import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.Constants;
import java.io.IOException;
import java.util.Map;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

public class AuthenticationFilter implements Filter {

  private final AuthenticatorChain chain = new AuthenticatorChain(ImmutableMap.of(
      "shared_secret", "YouKnowNothing",
      "signing_key", "YouKnowNothing",
      "signing_alg", "HS256"
  ));

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    chain.register(new DataHubFrontendAuthenticator());
    chain.register(new DataHubTokenAuthenticator());
  }

  @Override
  public void doFilter(
      ServletRequest request,
      ServletResponse response,
      FilterChain chain)
      throws IOException, ServletException {

    com.datahub.authentication.AuthenticationContext context = buildAuthContext((HttpServletRequest) request);
    AuthenticationResult result = this.chain.authenticate(context);
    if (AuthenticationResult.Type.SUCCESS.equals(result.type())) {
      // Successfully authenticated.
      AuthenticationContext.setActor(result.username());
    } else {
      // Reject request
      // TODO: Return 401.
      throw new ServletException();
    }
    chain.doFilter(request, response);
    AuthenticationContext.remove();
  }

  private com.datahub.authentication.AuthenticationContext buildAuthContext(HttpServletRequest request) {
    com.datahub.authentication.AuthenticationContext context = new com.datahub.authentication.AuthenticationContext() {
      @Override
      public Map<String, String> headers() {
        return request.getHeaderNames().;
      }
    }
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
