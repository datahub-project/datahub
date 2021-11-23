package com.datahub.authorization;

import javax.annotation.Nonnull;

public class ResourceSpec {

  private final String _type;
  private final String _resource;

  public ResourceSpec(
      @Nonnull final String type,
      @Nonnull final String resource // urn:li:dataset:(123)
  ) {
    _type = type;
    _resource = resource;
  }

  public String getType() {
    return _type;
  }

  public String getResource() {
    return _resource;
  }
}