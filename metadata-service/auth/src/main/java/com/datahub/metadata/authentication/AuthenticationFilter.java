package com.datahub.metadata.authentication;

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
      principal = httpRequest.getHeader("X-DataHub-Actor");
    }
    if (principal != null) {
      // Save actor to ThreadLocal context.
      AuthenticationContext.setActor(principal);
    } else {
      // TODO: Remove DataHub as the default actor once authentication at metadata-service is complete.
      AuthenticationContext.setActor("urn:li:corpuser:datahub");
    }
    chain.doFilter(request, response);
    AuthenticationContext.remove();
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
