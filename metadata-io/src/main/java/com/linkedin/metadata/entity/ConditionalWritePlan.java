package com.linkedin.metadata.entity;

import com.linkedin.metadata.aspect.SystemAspect;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * The decision made for one aspect write BEFORE its version-0 write executes, produced by {@code
 * AspectDao.planConditionalWrite}. Splitting the plan from execution lets the OL persist loop batch
 * the version-0 CAS UPDATEs of many aspects into one JDBC {@code executeBatch} while still routing
 * inserts / legacy / no-op writes through the sequential path — and lets a single code path ({@code
 * saveLatestAspectConditional}) stay the source of truth for both the sequential and the batched
 * flows, so they cannot drift.
 *
 * <p>For {@link Kind#ELIGIBLE_CAS} the caller runs the conditional UPDATE (single-row or batched)
 * with {@code newAspect} + {@link #getExpectedVersion()}; on a win it applies the deferred history
 * row via {@code AspectDao.applyConditionalHistory} using {@link #getOldDbAspect()} / {@link
 * #getTargetVersion()} / {@link #isNeedsHistory()}.
 */
@Value
public class ConditionalWritePlan {

  public enum Kind {
    /**
     * Aspect + system metadata unchanged: no write, no MCL — a legitimate no-op (never a conflict).
     */
    NOOP,
    /** No existing version-0 row: unconditional initial insert (duplicate-key race → conflict). */
    INSERT_NEW,
    /**
     * Existing row whose stored systemMetadata has no version: last-writer-wins, no CAS predicate.
     */
    LEGACY_UNCONDITIONAL,
    /** Existing versioned row: guarded by a version-0 CAS on {@link #getExpectedVersion()}. */
    ELIGIBLE_CAS
  }

  @Nonnull Kind kind;

  // ELIGIBLE_CAS only ------------------------------------------------------------------------
  /** Stored version-0 systemMetadata version the CAS predicate must match. */
  @Nullable String expectedVersion;

  /** The current version-0 row, captured before the CAS overwrites it — the history-row payload. */
  @Nullable SystemAspect oldDbAspect;

  /** The version number a surviving history row is written at. */
  long targetVersion;

  /** Whether a version-N history row should be written on a winning CAS. */
  boolean needsHistory;

  @Nonnull
  public static ConditionalWritePlan of(@Nonnull Kind kind) {
    return new ConditionalWritePlan(kind, null, null, 0L, false);
  }

  @Nonnull
  public static ConditionalWritePlan eligibleCas(
      @Nonnull String expectedVersion,
      @Nonnull SystemAspect oldDbAspect,
      long targetVersion,
      boolean needsHistory) {
    return new ConditionalWritePlan(
        Kind.ELIGIBLE_CAS, expectedVersion, oldDbAspect, targetVersion, needsHistory);
  }
}
