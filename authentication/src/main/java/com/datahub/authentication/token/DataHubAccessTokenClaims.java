package com.datahub.authentication.token;

import java.util.Objects;


/**
 * Contains strongly-typed claims that appear in all DataHub granted access tokens.
 */
public class DataHubAccessTokenClaims {

  private final String actorUrn;

  public DataHubAccessTokenClaims(final String actorUrn) {
    Objects.requireNonNull(actorUrn);
    this.actorUrn = actorUrn;
  }

  public String getActorUrn() {
    return this.actorUrn;
  }

}
