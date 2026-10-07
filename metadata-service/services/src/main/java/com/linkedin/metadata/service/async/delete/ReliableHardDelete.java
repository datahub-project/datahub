package com.linkedin.metadata.service.async.delete;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
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
 * <p>It only orders today's code: it captures every aspect's version, runs today's reference
 * cleanup with each referrer write conditional on the version read, then today's {@code deleteUrn}
 * bounded by the captured versions. Data written after the capture survives. Repeating a failed
 * request is safe; a repeated request captures the versions again, so it also deletes what was
 * written before it.
 *
 * <p>On Cassandra the latest rows are not locked, so the bound is best-effort there.
 */
public class ReliableHardDelete {
  private final EntityService<?> entityService;
  private final DeleteEntityService deleteEntityService;
  private final boolean enabled;

  public ReliableHardDelete(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final DeleteEntityService deleteEntityService,
      final boolean enabled) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
    this.deleteEntityService = Objects.requireNonNull(deleteEntityService, "deleteEntityService");
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Hard-deletes {@code urn}; the caller has already authorized it. A delete today's checks reject
   * throws before anything changes; a reference that cannot be removed throws before the entity is
   * touched.
   */
  @Nonnull
  public DeleteEntityReport delete(@Nonnull OperationContext opContext, @Nonnull final Urn urn) {
    // References go first, so the checks the entity delete would fail on run before them.
    entityService.validateHardDelete(opContext, urn);
    final Optional<DeleteCeiling> ceiling = entityService.captureDeleteCeiling(opContext, urn);
    if (ceiling.isEmpty()) {
      return DeleteEntityReport.alreadyDeleted(urn);
    }
    final Integer referencesRemoved =
        deleteEntityService.deleteReferencesToOrFail(opContext, urn).getTotal();

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
      outcome = ConditionalDeleteOutcome.PARTIAL;
    }
    return new DeleteEntityReport(
        urn.toString(),
        outcome,
        (keyRows == null ? 0 : keyRows) + otherRows,
        referencesRemoved == null ? 0 : referencesRemoved);
  }
}
