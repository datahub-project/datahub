package com.linkedin.metadata.entity;

import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Value;

@Value
public class TransactionResult<T> {
  public static TransactionResult<IngestAspectsResult> ingestAspectsRollback() {
    return new TransactionResult<>(Optional.of(IngestAspectsResult.EMPTY), false);
  }

  public static TransactionResult<IngestAspectsResult> of(@Nonnull IngestAspectsResult results) {
    return new TransactionResult<>(Optional.of(results), results.shouldCommit());
  }

  public static <T> TransactionResult<T> rollback() {
    return new TransactionResult<>(Optional.empty(), false);
  }

  public static <T> TransactionResult<T> commit(@Nonnull T results) {
    return new TransactionResult<>(Optional.of(results), true);
  }

  @Nonnull Optional<T> results;
  boolean commitOrRollback;
}
