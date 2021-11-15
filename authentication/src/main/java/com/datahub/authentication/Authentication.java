package com.datahub.authentication;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


/**
 * Class representing an authenticated actor accessing DataHub.
 */
public class Authentication {

  private final Actor authenticatedActor;
  private final String credentials;
  private final Set<String> groups;
  private final Map<String, Object> claims;
  private final Actor delegatedForActor;

  public Authentication(
      @Nonnull final Actor authenticatedActor,
      @Nonnull final String credentials,
      @Nonnull final Set<String> groups,
      @Nonnull final Map<String, Object> claims,
      @Nullable final Actor delegatedForActor) {
    this.authenticatedActor = Objects.requireNonNull(authenticatedActor);
    this.credentials = Objects.requireNonNull(credentials);
    this.groups = Objects.requireNonNull(groups);
    this.claims = Objects.requireNonNull(claims);
    this.delegatedForActor = delegatedForActor;
  }

  /**
   * Returns the authenticated actor
   */
  public Actor getAuthenticatedActor() {
    return this.authenticatedActor;
  }

  /**
   * Returns the credentials associated with the current request (e.g. the value of the Authorization header)
   */
  public String getCredentials() {
    return this.credentials;
  }

  /**
   * Returns an optional "delegated for" actor, which is an actor who is being accessed on behalf of.
   * This can occur if the internal system is making a call on behalf of an external user.
   */
  public Optional<Actor> getDelegatedForActor() {
    return Optional.ofNullable(this.delegatedForActor);
  }
}
