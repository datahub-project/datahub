package com.linkedin.metadata.entity;

import io.ebean.DuplicateKeyException;
import io.ebean.Transaction;
import java.util.ArrayList;
import java.util.List;
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
