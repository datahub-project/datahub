package com.linkedin.metadata.throttle;

/** Identifies where in the GMS request path throttling was applied. */
public enum ThrottleResponseSource {
  SERVLET_FILTER("servlet-filter"),
  GRAPHQL_GATE("graphql-gate"),
  METADATA_WRITE("metadata-write"),
  OPENSEARCH("opensearch");

  private final String headerValue;

  ThrottleResponseSource(String headerValue) {
    this.headerValue = headerValue;
  }

  public String headerValue() {
    return headerValue;
  }
}
