package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Result of a single aspect write on the optimistic-locking path. Identity is carried inline
 * ({@code urn} + {@code aspectName}) so a {@link BatchWriteResult} can group results by URN /
 * branch without a separate key type.
 *
 * <p>Effectively immutable (all fields final), but intentionally NOT annotated {@code @Immutable}:
 * the FAILED variant carries a {@link Throwable}, which is mutable, so the strict contract would
 * not hold.
 */
public final class AspectWriteResult {

  @Nonnull private final Urn urn;
  @Nonnull private final String aspectName;
  @Nonnull private final AspectWriteOutcome outcome;
  @Nullable private final Long committedVersion;
  @Nullable private final Throwable error;

  private AspectWriteResult(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull AspectWriteOutcome outcome,
      @Nullable Long committedVersion,
      @Nullable Throwable error) {
    this.urn = urn;
    this.aspectName = aspectName;
    this.outcome = outcome;
    this.committedVersion = committedVersion;
    this.error = error;
  }

  @Nonnull
  public static AspectWriteResult committed(
      @Nonnull Urn urn, @Nonnull String aspectName, long committedVersion) {
    return new AspectWriteResult(
        urn, aspectName, AspectWriteOutcome.COMMITTED, committedVersion, null);
  }

  @Nonnull
  public static AspectWriteResult noop(@Nonnull Urn urn, @Nonnull String aspectName) {
    return new AspectWriteResult(urn, aspectName, AspectWriteOutcome.NOOP, null, null);
  }

  @Nonnull
  public static AspectWriteResult conflict(@Nonnull Urn urn, @Nonnull String aspectName) {
    return new AspectWriteResult(urn, aspectName, AspectWriteOutcome.CONFLICT, null, null);
  }

  @Nonnull
  public static AspectWriteResult failed(
      @Nonnull Urn urn, @Nonnull String aspectName, @Nullable Throwable error) {
    return new AspectWriteResult(urn, aspectName, AspectWriteOutcome.FAILED, null, error);
  }

  @Nonnull
  public Urn getUrn() {
    return urn;
  }

  @Nonnull
  public String getAspectName() {
    return aspectName;
  }

  @Nonnull
  public AspectWriteOutcome getOutcome() {
    return outcome;
  }

  /** The committed version-0 row version. Present only when {@link #getOutcome()} is COMMITTED. */
  @Nullable
  public Long getCommittedVersion() {
    return committedVersion;
  }

  /** The non-retryable cause. Present only when {@link #getOutcome()} is FAILED. */
  @Nullable
  public Throwable getError() {
    return error;
  }

  public boolean isConflict() {
    return outcome == AspectWriteOutcome.CONFLICT;
  }
}
