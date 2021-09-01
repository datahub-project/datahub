package com.datahub.metadata.authorization;

import java.util.Optional;


public class AuthorizationRequest {

  private final String _principal;
  private final String _privilege;
  private final Optional<ResourceSpec> _resourceSpec;

  public AuthorizationRequest(
      final String principal, // urn:li:corpuser:datahub
      final String privilege,
      final Optional<ResourceSpec> resourceSpec) {
    _principal = principal;
    _privilege = privilege;
    _resourceSpec = resourceSpec;
  }

  public String principal() {
    return _principal;
  }

  public String privilege() {
    return _privilege;
  }

  public Optional<ResourceSpec> resourceSpec() {
    return _resourceSpec;
  }
}
