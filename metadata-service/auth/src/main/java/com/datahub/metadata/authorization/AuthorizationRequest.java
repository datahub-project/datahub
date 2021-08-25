package com.datahub.metadata.authorization;

import java.util.Optional;
import javax.annotation.Nonnull;


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

  public static class ResourceSpec {

    private final String _type;
    private final String _urn;

    public ResourceSpec(
        @Nonnull final String type,
        @Nonnull final String urn // urn:li:dataset:(123)
        // final String domain
        // final String platform - or an additional attributes bag.
      ) {
      _type = type;
      _urn = urn;
    }

    public String getType() {
      return _type;
    }

    public String getUrn() {
      return _urn;
    }
  }
}
