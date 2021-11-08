package com.datahub.metadata.authentication;

import com.datahub.authentication.AuthenticationResult;
import com.datahub.authentication.AuthenticatorChain;
import com.datahub.authentication.authenticators.DataHubTokenAuthenticator;
import com.datahub.authentication.authenticators.DataHubSystemAuthenticator;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class AuthenticationFilter implements Filter {

  // TODO: Figure out the best way to handle filter chain configuration.
  private final AuthenticatorChain chain = new AuthenticatorChain(ImmutableMap.of(
      "signing_key", "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=",
      "signing_alg", "HS256",
      "system_client_id","__datahub_frontend",
      "system_client_secret","YouKnowNothing"
  ));

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    chain.register(new DataHubTokenAuthenticator());
    chain.register(new DataHubSystemAuthenticator());
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
      System.out.println(String.format("Setting authentication context %s", result.authentication().getCredentials()));

      AuthenticationContext.setAuthentication(result.authentication());
      chain.doFilter(request, response);
    } else {
      // Reject request
      // TODO: Return 401.
      ((HttpServletResponse) response).sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized to perform this action.");
      return;
    }
    AuthenticationContext.remove();
  }

  private com.datahub.authentication.AuthenticationContext buildAuthContext(HttpServletRequest request) {
    return () -> Collections.list(request.getHeaderNames())
        .stream()
        .collect(Collectors.toMap(headerName -> headerName, request::getHeader));
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
