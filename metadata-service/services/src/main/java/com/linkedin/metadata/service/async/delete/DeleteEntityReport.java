package com.linkedin.metadata.service.async.delete;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.RollbackRunResult;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * What one reliable hard delete of an entity did.
 *
 * @param rowsDeleted rows the entity delete removed, as {@code deleteUrn} counts them; when the
 *     entity stays ({@code PARTIAL}), one per aspect deleted
 * @param rollbackRunResult what {@code deleteUrn} returned, for callers that respond with it
 */
public record DeleteEntityReport(
    @Nonnull String urn,
    @Nonnull ConditionalDeleteOutcome outcome,
    long rowsDeleted,
    @Nonnull RollbackRunResult rollbackRunResult) {

  public DeleteEntityReport {
    Objects.requireNonNull(urn, "urn");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(rollbackRunResult, "rollbackRunResult");
  }

  /** {@code rollbackRunResult} is what {@code deleteUrn} returns for an absent entity. */
  @Nonnull
  public static DeleteEntityReport alreadyDeleted(@Nonnull Urn urn) {
    return new DeleteEntityReport(
        urn.toString(),
        ConditionalDeleteOutcome.ALREADY_DELETED,
        0L,
        new RollbackRunResult(List.of(), 0, List.of()));
  }
}
