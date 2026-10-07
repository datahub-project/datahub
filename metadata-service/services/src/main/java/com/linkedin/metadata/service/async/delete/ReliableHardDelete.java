package com.linkedin.metadata.service.async.delete;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The hard delete of one entity behind the {@code featureFlags.reliableHardDelete} switch, used by
 * the Rest.li and OpenAPI entity deletes and by GMS's entity client (the GraphQL deletes). Callers
 * check {@link #isEnabled()} and run their existing code when it is off.
 *
 * <p>It is today's {@code deleteUrn}, bounded by the aspect versions captured when the request
 * arrived: data written after the capture survives. When that keeps the entity, the request fails
 * with an {@link IllegalStateException}, so a caller does not go on to remove references to an
 * entity that still exists. References other entities hold are not touched; each caller keeps its
 * own cleanup. Repeating a failed request is safe; a repeated request captures the versions again,
 * so it also deletes what was written before it.
 *
 * <p>On Cassandra the latest rows are not locked, so the bound is best-effort there.
 */
public class ReliableHardDelete {
  private final EntityService<?> entityService;
  private final boolean enabled;

  public ReliableHardDelete(@Nonnull final EntityService<?> entityService, final boolean enabled) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * The aspect versions {@code urn} has now, which {@link #delete(OperationContext, Urn, Optional)}
   * bounds the delete by; empty when the entity does not exist. A request deleting several entities
   * captures all of them before deleting any, so each bound is the state at arrival.
   */
  @Nonnull
  public Optional<DeleteCeiling> capture(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    return entityService.captureDeleteCeiling(opContext, urn);
  }

  /**
   * Hard-deletes {@code urn}; the caller has already authorized it.
   *
   * @throws IllegalStateException when the entity was written to while being deleted and so still
   *     exists; the captured aspects were deleted, and repeating the request finishes the delete
   */
  @Nonnull
  public DeleteEntityReport delete(@Nonnull OperationContext opContext, @Nonnull final Urn urn) {
    return delete(opContext, urn, capture(opContext, urn));
  }

  /**
   * {@link #delete(OperationContext, Urn)} bounded by a {@code ceiling} the caller already took
   * with {@link #capture}; an empty one means the entity did not exist.
   */
  @Nonnull
  public DeleteEntityReport delete(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Optional<DeleteCeiling> ceiling) {
    if (ceiling.isEmpty()) {
      return DeleteEntityReport.alreadyDeleted(urn);
    }

    final RollbackRunResult deleted = entityService.deleteUrn(opContext, urn, ceiling.get());
    final boolean keyDeleted =
        deleted.getRollbackResults().stream()
            .map(RollbackResult::getKeyAffected)
            .anyMatch(Boolean.TRUE::equals);
    final long otherRows = keyDeleted ? 0 : deleted.getRollbackResults().size();
    final Integer keyRows = deleted.getRowsDeletedFromEntityDeletion();
    final ConditionalDeleteOutcome outcome;
    if (keyDeleted) {
      outcome = ConditionalDeleteOutcome.DELETED;
    } else if (entityService.captureDeleteCeiling(opContext, urn).isEmpty()) {
      // Not deleted by this request, but gone: a concurrent request deleted it.
      outcome = ConditionalDeleteOutcome.ALREADY_DELETED;
    } else {
      throw new IllegalStateException(
          String.format(
              "Hard delete of %s did not complete: it was written to while being deleted; try the"
                  + " delete again",
              urn));
    }
    return new DeleteEntityReport(
        urn.toString(), outcome, (keyRows == null ? 0 : keyRows) + otherRows, deleted);
  }
}
