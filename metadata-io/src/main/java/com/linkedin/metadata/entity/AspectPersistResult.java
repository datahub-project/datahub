package com.linkedin.metadata.entity;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Return type of the singular persist step ({@code EntityServiceImpl.ingestAspectToLocalDB}) on the
 * optimistic-locking path. It couples the {@link AspectWriteOutcome} (conflict expressed as data,
 * never as control flow) with the {@link UpdateAspectResult} that downstream retention / MCL
 * emission consumes.
 *
 * <p>This wrapper lives in {@code metadata-io} on purpose: {@link UpdateAspectResult} is defined in
 * the {@code metadata-service/services} module, which {@code metadata-io} depends on (not the other
 * way round), so the outcome enum cannot be added onto {@link UpdateAspectResult} itself without an
 * illegal reverse dependency. Keeping the pairing here preserves the module boundary.
 */
@Immutable
public final class AspectPersistResult {

  @Nonnull private final AspectWriteOutcome outcome;
  @Nullable private final UpdateAspectResult result;

  private AspectPersistResult(
      @Nonnull AspectWriteOutcome outcome, @Nullable UpdateAspectResult result) {
    this.outcome = outcome;
    this.result = result;
  }

  /** A committed write carrying the {@link UpdateAspectResult} for retention / MCL emission. */
  @Nonnull
  public static AspectPersistResult committed(@Nonnull UpdateAspectResult result) {
    // COMMITTED must carry a result — fail fast here rather than deferring an NPE to a downstream
    // retention / MCL consumer if a caller ever passes null.
    return new AspectPersistResult(
        AspectWriteOutcome.COMMITTED, Objects.requireNonNull(result, "committed result required"));
  }

  /** Aspect + system metadata unchanged; nothing written and nothing to retry. */
  @Nonnull
  public static AspectPersistResult noop() {
    return new AspectPersistResult(AspectWriteOutcome.NOOP, null);
  }

  /** Version-0 CAS missed — eligible for scoped (branch-keyed) retry. */
  @Nonnull
  public static AspectPersistResult conflict() {
    return new AspectPersistResult(AspectWriteOutcome.CONFLICT, null);
  }

  @Nonnull
  public AspectWriteOutcome getOutcome() {
    return outcome;
  }

  /** Present only when {@link #getOutcome()} is {@link AspectWriteOutcome#COMMITTED}. */
  @Nullable
  public UpdateAspectResult getResult() {
    return result;
  }
}
