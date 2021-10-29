package com.datahub.authentication;

import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Stores the result returned by a single {@link Authenticator}.
 */
public class AuthenticationResult {
  public enum Type {
    SUCCESS,
    FAILURE
  }
  private final Type type;
  private final String username; // The resolved DataHub username, (without urn:li:datahub)
  private final Set<String> groups;
  private final Map<String, Object> claims;

  public AuthenticationResult(
      Type type,
      String username,
      Set<String> groups,
      Map<String, Object> claims) {
    this.type = type;
    this.username = username;
    this.groups = groups;
    this.claims = claims;
  }

  public Type type() {
    return this.type;
  }

  public String username() {
    return this.username;
  }

  public Set<String> groups() {
    return this.groups;
  }

  public Optional<Set<String>> maybeGroups() {
    return Optional.of(this.groups);
  }

}
