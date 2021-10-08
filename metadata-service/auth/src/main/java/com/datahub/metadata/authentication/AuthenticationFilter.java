package com.datahub.metadata.authentication;

import com.linkedin.metadata.Constants;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import static com.linkedin.metadata.Constants.*;


// TODO: Add filter to Rest.li servlet as well.
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
      principal = httpRequest.getHeader(Constants.ACTOR_HEADER_NAME);
    }
    if (principal != null) {
      // Save actor to ThreadLocal context.
      AuthenticationContext.setActor(principal);
    } else {
      AuthenticationContext.setActor(Constants.UNKNOWN_ACTOR);
    }
    chain.doFilter(request, response);
    AuthenticationContext.remove();
  }

  @Override
  public void destroy() {
    // Nothing
  }
}
