package com.linkedin.metadata.throttle;

/** Standard HTTP response headers for DataHub throttling and rate limiting. */
public final class ThrottleResponseHeaders {
  public static final String RULE = "X-DataHub-RateLimit-Rule";
  public static final String TYPE = "X-DataHub-RateLimit-Type";
  public static final String SOURCE = "X-DataHub-RateLimit-Source";
  public static final String ENDPOINT_RULE = "X-DataHub-RateLimit-Endpoint-Rule";
  public static final String RETRY_AFTER = "Retry-After";

  private ThrottleResponseHeaders() {}
}
