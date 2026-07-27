package com.linkedin.metadata.entity.ebean;

import com.linkedin.metadata.config.TransactionRetryConfiguration;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionRetryPolicy {

  private final Set<String> backoffSqlStates;
  private final Set<Integer> backoffVendorCodes;
  private final long initialBackoffMs;
  private final long maxBackoffMs;
  private final boolean jitterEnabled;

  public TransactionRetryPolicy(@Nonnull TransactionRetryConfiguration config) {
    this(config, true);
  }

  /** Test constructor — set {@code jitterEnabled=false} for deterministic backoff. */
  public TransactionRetryPolicy(
      @Nonnull TransactionRetryConfiguration config, boolean jitterEnabled) {
    this.backoffSqlStates = parseSqlStates(config.getBackoffSqlStates());
    this.backoffVendorCodes = parseVendorCodes(config.getBackoffVendorCodes());
    this.initialBackoffMs =
        config.getInitialBackoffMs() > 0
            ? config.getInitialBackoffMs()
            : TransactionRetryConfiguration.DEFAULT_INITIAL_BACKOFF_MS;
    this.maxBackoffMs =
        config.getMaxBackoffMs() > 0
            ? config.getMaxBackoffMs()
            : TransactionRetryConfiguration.DEFAULT_MAX_BACKOFF_MS;
    this.jitterEnabled = jitterEnabled;
  }

  public boolean shouldBackoff(@Nullable Throwable throwable) {
    return SqlTransientExceptionClassifier.isBackoffEligible(
        throwable, backoffSqlStates, backoffVendorCodes);
  }

  @Nullable
  public SQLException findMatchingSqlError(@Nullable Throwable throwable) {
    return SqlTransientExceptionClassifier.findBackoffSqlError(
        throwable, backoffSqlStates, backoffVendorCodes);
  }

  /**
   * Exponential backoff with optional jitter: {@code min(max, initial * 2^attempt) * U(0.5, 1.5)}.
   *
   * @param attempt zero-based retry attempt index (0 = first retry after initial failure)
   */
  public long backoffMillis(int attempt) {
    long base = Math.min(maxBackoffMs, initialBackoffMs * (1L << Math.min(attempt, 30)));
    if (!jitterEnabled) {
      return base;
    }
    double factor = 0.5 + ThreadLocalRandom.current().nextDouble();
    return Math.max(0L, Math.round(base * factor));
  }

  @Nonnull
  private static Set<String> parseSqlStates(@Nullable String csv) {
    if (csv == null || csv.isBlank()) {
      return Collections.emptySet();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toUnmodifiableSet());
  }

  @Nonnull
  private static Set<Integer> parseVendorCodes(@Nullable String csv) {
    if (csv == null || csv.isBlank()) {
      return Collections.emptySet();
    }
    Set<Integer> codes = new LinkedHashSet<>();
    for (String token : csv.split(",")) {
      String trimmed = token.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        codes.add(Integer.parseInt(trimmed));
      } catch (NumberFormatException e) {
        log.warn("Ignoring invalid ebean.transactionRetry.backoffVendorCodes entry '{}'", trimmed);
      }
    }
    return Collections.unmodifiableSet(codes);
  }
}
