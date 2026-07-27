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

  private String backoffSqlStates = DEFAULT_BACKOFF_SQL_STATES;
  private String backoffVendorCodes = DEFAULT_BACKOFF_VENDOR_CODES;
  private long initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS;
  private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
}
