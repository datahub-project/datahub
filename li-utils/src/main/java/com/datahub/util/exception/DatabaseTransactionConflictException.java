package com.datahub.util.exception;

import javax.annotation.Nullable;

/**
 * Thrown when transaction retries are exhausted on a backoff-eligible (deadlock / serialization)
 * failure. Clients should treat this as retryable.
 */
public class DatabaseTransactionConflictException extends RetryLimitReached {

  public static final String CODE = "DATABASE_TRANSACTION_CONFLICT";

  /** Default Retry-After hint (seconds) when config does not supply a positive value. */
  public static final long DEFAULT_RETRY_AFTER_SECONDS = 1L;

  @Nullable private final String sqlState;
  private final long retryAfterSeconds;

  public DatabaseTransactionConflictException(String message, @Nullable String sqlState) {
    this(message, sqlState, null, DEFAULT_RETRY_AFTER_SECONDS);
  }

  public DatabaseTransactionConflictException(
      String message, @Nullable String sqlState, Throwable cause) {
    this(message, sqlState, cause, DEFAULT_RETRY_AFTER_SECONDS);
  }

  public DatabaseTransactionConflictException(
      String message, @Nullable String sqlState, Throwable cause, long retryAfterSeconds) {
    super(message, cause);
    this.sqlState = sqlState;
    this.retryAfterSeconds =
        retryAfterSeconds > 0 ? retryAfterSeconds : DEFAULT_RETRY_AFTER_SECONDS;
  }

  public String getCode() {
    return CODE;
  }

  @Nullable
  public String getSqlState() {
    return sqlState;
  }

  /** Suggested client wait before retry (HTTP Retry-After / Rest.li header). */
  public long getRetryAfterSeconds() {
    return retryAfterSeconds;
  }

  public boolean isRetryable() {
    return true;
  }
}
