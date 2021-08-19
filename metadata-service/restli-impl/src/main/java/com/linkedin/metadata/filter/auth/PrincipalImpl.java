package com.linkedin.metadata.filter.auth;

import java.util.Map;


public class PrincipalImpl implements Principal {

  private final String name;
  private final Map<String, Object> attributes;

  public PrincipalImpl(String name, Map<String, Object> attributes) {
    this.name = name;
    this.attributes = attributes;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public Map<String, Object> attributes() {
    return this.attributes;
  }
}
