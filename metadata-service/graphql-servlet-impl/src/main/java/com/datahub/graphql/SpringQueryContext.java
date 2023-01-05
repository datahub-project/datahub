package com.datahub.graphql;

import com.datahub.authentication.Authentication;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;


public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final Authentication authentication;
  private final Authorizer authorizer;

  public SpringQueryContext(final boolean isAuthenticated, final Authentication authentication, final Authorizer authorizer) {
    this.isAuthenticated = isAuthenticated;
    this.authentication = authentication;
    this.authorizer = authorizer;
  }

  @Override
  public boolean isAuthenticated() {
    return this.isAuthenticated;
  }

  @Override
  public Authentication getAuthentication() {
    return this.authentication;
  }

  @Override
  public Authorizer getAuthorizer() {
    return this.authorizer;
  }
}
