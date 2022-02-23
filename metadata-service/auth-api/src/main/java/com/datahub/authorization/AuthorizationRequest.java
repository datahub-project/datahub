package com.datahub.authorization;

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
}
