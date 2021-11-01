package com.datahub.authentication;

import java.util.Map;
import java.util.Set;


/**
 * Class representing an authenticated actor inside DataHub.
 *
 * Each actor, regardless of whether they are users or systems, are given an actor urn.
 *
 * TODO: Add reference to authenticator name for tracking.
 */
public class Authentication {

  private final String credentials;
  private final String actorUrn;
  private final String delegatorUrn;
  private final Set<String> groups;
  private final Map<String, Object> claims;

  public Authentication(final String credentials, final String actorUrn) {
    this.credentials = credentials;
    this.actorUrn = actorUrn;
    this.delegatorUrn = null;
    this.groups = null;
    this.claims = null;
  }

  public Authentication(
      final String credentials,
      final String actorUrn,
      final String delegatorUrn,
      final Set<String> groups,
      final Map<String, Object> claims) {
    this.credentials = credentials;
    this.actorUrn = actorUrn;
    this.delegatorUrn = delegatorUrn;
    this.groups = groups;
    this.claims = claims;
  }

  /**
   * Returns the urn associated with the current actor.
   */
  public String getActorUrn() {
    return this.actorUrn;
  }

  /**
   * Returns the credentials (Authorization header) associated with the current request.
   */
  public String getCredentials() {
    return this.credentials;
  }

  /**
   * Returns the urn associated with the user that the request is delegated on behalf of.
   * Only used in system internal calls.
   */
  public String getDelegatorUrn() {
    return this.delegatorUrn;
  }
}
