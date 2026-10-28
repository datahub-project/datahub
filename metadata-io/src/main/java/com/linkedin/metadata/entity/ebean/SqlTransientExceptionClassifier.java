package com.linkedin.metadata.entity.ebean;

import java.sql.SQLException;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class SqlTransientExceptionClassifier {

  private SqlTransientExceptionClassifier() {}

  /**
   * Walks {@link Throwable#getCause()} only to locate the first {@link SQLException}. Allowlist
   * matching uses {@link #findBackoffSqlError}, which additionally walks {@link
   * SQLException#getNextException()} on that first SQL exception — intentional for standard JDBC
   * wrapping.
   */
  @Nullable
  public static SQLException findSqlError(@Nullable Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof SQLException sqlException) {
        return sqlException;
      }
      current = current.getCause();
    }
    return null;
  }

  /**
   * Returns the first {@link SQLException} in the cause / {@code nextException} chain that matches
   * the backoff allowlist, or null if none match.
   */
  @Nullable
  public static SQLException findBackoffSqlError(
      @Nullable Throwable throwable,
      @Nonnull Set<String> sqlStates,
      @Nonnull Set<Integer> vendorCodes) {
    SQLException sqlException = findSqlError(throwable);
    while (sqlException != null) {
      if (matchesAllowlist(sqlException, sqlStates, vendorCodes)) {
        return sqlException;
      }
      sqlException = sqlException.getNextException();
    }
    return null;
  }

  public static boolean isBackoffEligible(
      @Nullable Throwable throwable,
      @Nonnull Set<String> sqlStates,
      @Nonnull Set<Integer> vendorCodes) {
    return findBackoffSqlError(throwable, sqlStates, vendorCodes) != null;
  }

  private static boolean matchesAllowlist(
      @Nonnull SQLException sqlException,
      @Nonnull Set<String> sqlStates,
      @Nonnull Set<Integer> vendorCodes) {
    String sqlState = sqlException.getSQLState();
    if (sqlState != null && sqlStates.contains(sqlState)) {
      return true;
    }
    return vendorCodes.contains(sqlException.getErrorCode());
  }
}
