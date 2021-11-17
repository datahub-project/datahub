package com.datahub.authentication;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;


/**
 * Class representing an authenticated actor accessing DataHub.
 */
public class Authentication {

  private final Actor authenticatedActor;
  private final String credentials;
  private final Map<String, Object> claims;
  private final String delegatedForActorUrn;

  public Authentication(
      @Nonnull final Actor authenticatedActor,
      @Nonnull final String credentials,
      @Nonnull final Map<String, Object> claims) {
    this(authenticatedActor, credentials, null, claims);
  }

  public Authentication(
      @Nonnull final Actor authenticatedActor,
      @Nonnull final String credentials,
      @Nullable final String delegatedForActorUrn,
      @Nonnull final Map<String, Object> claims) {
    this.authenticatedActor = Objects.requireNonNull(authenticatedActor);
    this.credentials = Objects.requireNonNull(credentials);
    this.delegatedForActorUrn = delegatedForActorUrn;
    this.claims = Objects.requireNonNull(claims);
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
  public Optional<String> getDelegatedForActorUrn() {
    return Optional.ofNullable(this.delegatedForActorUrn);
  }

  /**
   * Returns an arbitrary set of claims resolved by the Authenticator
   */
  public Map<String, Object> getClaims() {
    return this.claims;
  }

}
