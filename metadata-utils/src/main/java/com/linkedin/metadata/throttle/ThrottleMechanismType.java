package com.linkedin.metadata.throttle;

/** Identifies which throttling mechanism produced a 429 response. */
public enum ThrottleMechanismType {
  CAPACITY("capacity"),
  ENDPOINT("endpoint"),
  INGEST("ingest"),
  SEARCH("search");

  private final String headerValue;

  ThrottleMechanismType(String headerValue) {
    this.headerValue = headerValue;
  }

  public String headerValue() {
    return headerValue;
  }
}
