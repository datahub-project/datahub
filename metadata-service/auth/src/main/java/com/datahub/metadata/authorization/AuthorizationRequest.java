package com.datahub.metadata.authorization;

import java.util.Optional;


public class AuthorizationRequest {

  private final String _actor;
  private final String _privilege;
  private final Optional<ResourceSpec> _resourceSpec;

  public AuthorizationRequest(
      final String actor, // urn:li:corpuser:datahub
      final String privilege,
      final Optional<ResourceSpec> resourceSpec) {
    _actor = actor;
    _privilege = privilege;
    _resourceSpec = resourceSpec;
  }

  public String actor() {
    return _actor;
  }

  public String privilege() {
    return _privilege;
  }

  public Optional<ResourceSpec> resourceSpec() {
    return _resourceSpec;
  }
}
