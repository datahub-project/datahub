package com.datahub.util.exception;

import javax.annotation.Nullable;

/**
 * Thrown when transaction retries are exhausted on a backoff-eligible (deadlock / serialization)
 * failure. Clients should treat this as retryable.
 */
public class DatabaseTransactionConflictException extends RetryLimitReached {

  public static final String CODE = "DATABASE_TRANSACTION_CONFLICT";

  @Nullable private final String sqlState;

  public DatabaseTransactionConflictException(String message, @Nullable String sqlState) {
    super(message);
    this.sqlState = sqlState;
  }

  public DatabaseTransactionConflictException(
      String message, @Nullable String sqlState, Throwable cause) {
    super(message, cause);
    this.sqlState = sqlState;
  }

  public String getCode() {
    return CODE;
  }

  @Nullable
  public String getSqlState() {
    return sqlState;
  }

  public boolean isRetryable() {
    return true;
  }
}
