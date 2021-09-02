package com.datahub.metadata.graphql;

import com.linkedin.datahub.graphql.QueryContext;


public class SpringQueryContext implements QueryContext {

  private final boolean isAuthenticated;
  private final String actor;

  public SpringQueryContext(final boolean isAuthenticated, final String actor) {
    this.isAuthenticated = isAuthenticated;
    this.actor = actor;
  }

  @Override
  public boolean isAuthenticated() {
    return this.isAuthenticated;
  }

  @Override
  public String getActor() {
    return this.actor;
  }
}
