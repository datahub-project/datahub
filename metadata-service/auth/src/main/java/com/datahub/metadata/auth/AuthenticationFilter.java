package com.datahub.metadata.auth;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;


public class AuthenticationFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // Nothing
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    String principal = null;
    if (request instanceof HttpServletRequest) {
      HttpServletRequest httpRequest = (HttpServletRequest) request;
      principal = httpRequest.getHeader("X-DataHub-Principal");
    }
    if (principal != null) {
      // Save actor to ThreadLocal context.
      AuthContext.setPrincipal(principal);
    } else {
      // TODO: Remove DataHub as the default actor once authentication at metadata-service is complete.
      AuthContext.setPrincipal("urn:li:corpuser:datahub");
    }
    chain.doFilter(request, response);
    AuthContext.remove();
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
