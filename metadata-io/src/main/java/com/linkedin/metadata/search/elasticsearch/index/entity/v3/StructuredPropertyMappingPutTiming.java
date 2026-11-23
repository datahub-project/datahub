package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

/**
 * How long a structured-property mapping put is allowed to run. The search trace uses the same
 * numbers so a write stays pending while that put, and the following system-metadata refresh, can
 * still be in flight after the consumer offset has moved past it.
 */
public final class StructuredPropertyMappingPutTiming {

  public static final int MAX_ATTEMPTS = 2;
  public static final int SOCKET_TIMEOUT_MILLIS = 60_000;
  public static final int CONNECTION_REQUEST_TIMEOUT_MILLIS = 10_000;
  public static final long RETRY_BACKOFF_MILLIS = 1_000L;
  public static final long BUDGET_MILLIS = 150_000L;

  private StructuredPropertyMappingPutTiming() {}

  /**
   * Refresh interval plus the mapping put allowance: each attempt's socket and connection-request
   * timeouts, the backoff between attempts, and never longer than the put budget.
   */
  public static long searchCommitGraceMillis(long refreshIntervalMillis) {
    long retries = Math.max(0, MAX_ATTEMPTS - 1L);
    long putAllowance =
        (long) MAX_ATTEMPTS * (SOCKET_TIMEOUT_MILLIS + CONNECTION_REQUEST_TIMEOUT_MILLIS)
            + RETRY_BACKOFF_MILLIS * retries;
    return Math.min(putAllowance, BUDGET_MILLIS) + Math.max(0L, refreshIntervalMillis);
  }
}
