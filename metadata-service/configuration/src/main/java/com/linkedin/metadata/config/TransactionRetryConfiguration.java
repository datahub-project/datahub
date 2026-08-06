package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TransactionRetryConfiguration {
  public static final String DEFAULT_BACKOFF_SQL_STATES = "40001,40P01";
  public static final String DEFAULT_BACKOFF_VENDOR_CODES = "1213";
  public static final long DEFAULT_INITIAL_BACKOFF_MS = 50L;
  public static final long DEFAULT_MAX_BACKOFF_MS = 1000L;
  public static final long DEFAULT_RETRY_AFTER_SECONDS = 1L;

  @Builder.Default private String backoffSqlStates = DEFAULT_BACKOFF_SQL_STATES;
  @Builder.Default private String backoffVendorCodes = DEFAULT_BACKOFF_VENDOR_CODES;
  @Builder.Default private long initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS;
  @Builder.Default private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;

  /** HTTP / Rest.li Retry-After hint (seconds) when conflict retries are exhausted. */
  @Builder.Default private long retryAfterSeconds = DEFAULT_RETRY_AFTER_SECONDS;
}
