package com.datahub.authorization;

import java.util.Objects;
import java.util.Optional;

public class AuthorizationRequest {

  private final String _actorUrn;
  private final String _privilege;
  private final Optional<ResourceSpec> _resourceSpec;

  public AuthorizationRequest(
      final String actorUrn, // urn:li:corpuser:datahub, urn:li:corpGroup:engineering
      final String privilege,
      final Optional<ResourceSpec> resourceSpec) {
    _actorUrn = actorUrn;
    _privilege = privilege;
    _resourceSpec = resourceSpec;
  }

  public String actorUrn() {
    return _actorUrn;
  }

  public String privilege() {
    return _privilege;
  }

  public Optional<ResourceSpec> resourceSpec() {
    return _resourceSpec;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (!(o instanceof AuthorizationRequest)) {
      return false;
    }

    AuthorizationRequest c = (AuthorizationRequest) o;

    return this._actorUrn.equals(c._actorUrn)
        && this._privilege.equals(c._privilege)
        && this._resourceSpec.equals(c._resourceSpec);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        this._actorUrn,
        this._privilege,
        this._resourceSpec
    );
  }
}
