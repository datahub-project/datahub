package com.linkedin.metadata.entity;

import io.ebean.DuplicateKeyException;
import io.ebean.Transaction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.springframework.lang.Nullable;

/** Wrap the transaction with additional information about the exceptions during retry. */
@Data
@AllArgsConstructor
@Accessors(fluent = true)
public class TransactionContext {
  public static final int DEFAULT_MAX_TRANSACTION_RETRY = 3;
  public static final long RETRY_BACKOFF_BASE_MS = 50;
  public static final long RETRY_BACKOFF_MAX_MS = 2000;

  public static TransactionContext empty() {
    return empty(DEFAULT_MAX_TRANSACTION_RETRY);
  }

  public static TransactionContext empty(@Nullable Integer maxRetries) {
    return empty(null, maxRetries == null ? DEFAULT_MAX_TRANSACTION_RETRY : maxRetries);
  }

  public static TransactionContext empty(Transaction tx, int maxRetries) {
    return new TransactionContext(tx, maxRetries, new ArrayList<>());
  }

  @Nullable private Transaction tx;
  private int maxRetries;
  @NonNull private List<RuntimeException> exceptions;

  public TransactionContext success() {
    exceptions.clear();
    return this;
  }

  public TransactionContext addException(RuntimeException e) {
    exceptions.add(e);
    return this;
  }

  public int getFailedAttempts() {
    return exceptions.size();
  }

  @Nullable
  public RuntimeException lastException() {
    return exceptions.isEmpty() ? null : exceptions.get(exceptions.size() - 1);
  }

  public boolean lastExceptionIsDuplicateKey() {
    return lastException() instanceof DuplicateKeyException;
  }

  public boolean shouldAttemptRetry() {
    return exceptions.size() <= maxRetries;
  }

  /**
   * Sleeps with exponential backoff and jitter before the next retry attempt. Immediate lockstep
   * retries tend to re-collide with the concurrent transaction that caused the failure (e.g. a
   * deadlock victim retrying while its peer is still mid-transaction); jitter desynchronizes the
   * contenders so a retry can succeed.
   */
  public void backoffBeforeRetry() {
    long backoffMs = nextBackoffMs(exceptions.size());
    if (backoffMs > 0) {
      try {
        Thread.sleep(backoffMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Backoff for the given number of failed attempts: base * 2^(attempts-1), jittered to 50-150% of
   * that value, capped at {@link #RETRY_BACKOFF_MAX_MS}. Returns 0 for non-positive attempts.
   */
  static long nextBackoffMs(int failedAttempts) {
    if (failedAttempts <= 0) {
      return 0;
    }
    long exponential =
        Math.min(
            RETRY_BACKOFF_MAX_MS, RETRY_BACKOFF_BASE_MS * (1L << Math.min(failedAttempts - 1, 20)));
    double jitter = 0.5 + ThreadLocalRandom.current().nextDouble();
    return Math.min(RETRY_BACKOFF_MAX_MS, (long) (exponential * jitter));
  }

  public void commitAndContinue() {
    if (tx != null) {
      tx.commitAndContinue();
    }
    success();
  }

  public void flush() {
    if (tx != null) {
      tx.flush();
    }
  }

  public void rollback() {
    if (tx != null) {
      tx.rollback();
    }
  }
}
