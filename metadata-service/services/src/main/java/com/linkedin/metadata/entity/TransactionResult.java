/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
