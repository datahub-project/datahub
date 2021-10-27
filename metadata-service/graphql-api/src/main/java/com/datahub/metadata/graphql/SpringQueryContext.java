package com.datahub.metadata.graphql;

import com.datahub.metadata.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;


public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final String actor;
  private final Authorizer authorizer;

  public SpringQueryContext(final boolean isAuthenticated, final String actor, final Authorizer authorizer) {
    this.isAuthenticated = isAuthenticated;
    this.actor = actor;
    this.authorizer = authorizer;
  }

  @Override
  public boolean isAuthenticated() {
    return this.isAuthenticated;
  }

  @Override
  public String getActor() {
    return this.actor;
  }

  @Override
  public Authorizer getAuthorizer() {
    return this.authorizer;
  }
}
