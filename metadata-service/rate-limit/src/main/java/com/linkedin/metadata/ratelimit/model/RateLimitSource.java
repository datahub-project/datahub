package com.linkedin.metadata.ratelimit.model;

public enum RateLimitSource {
  SERVLET_FILTER("servlet-filter"),
  GRAPHQL_GATE("graphql-gate");

  private final String headerValue;

  RateLimitSource(String headerValue) {
    this.headerValue = headerValue;
  }

  public String headerValue() {
    return headerValue;
  }
}
