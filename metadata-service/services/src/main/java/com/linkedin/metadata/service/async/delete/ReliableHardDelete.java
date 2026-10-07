package com.linkedin.metadata.service.async.delete;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCascadeListener;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.CriterionUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * The reliable hard delete of one entity, shared by the Rest.li and OpenAPI entry points and by
 * GMS's entity client, through which the GraphQL deletes go. It carries the {@code
 * featureFlags.reliableHardDelete} kill switch: entry points check {@link #isEnabled()} and run
 * their existing code when it is off.
 *
 * <p>Runs on the calling thread:
 *
 * <ol>
 *   <li>Captures the ceiling: every aspect's version and the key's creation time, now. An absent
 *       entity is reported {@code ALREADY_DELETED} after its graph node and timeseries documents
 *       are removed: an earlier request may have deleted the entity and then failed before those.
 *   <li>Removes every reference to the entity; the first one that cannot be removed fails the
 *       request.
 *   <li>Deletes the entity up to the ceiling; then its graph node (only when the whole entity went)
 *       and its timeseries documents up to the capture time.
 * </ol>
 *
 * <p>Data written after the capture survives. Every step is idempotent, so repeating a failed
 * request finishes the work.
 */
@Slf4j
public class ReliableHardDelete {
  private final EntityService<?> entityService;
  private final DeleteEntityService deleteEntityService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final GraphService graphService;
  private final Clock clock;
  private final boolean enabled;

  public ReliableHardDelete(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final DeleteEntityService deleteEntityService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final GraphService graphService,
      @Nonnull final Clock clock,
      final boolean enabled) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
    this.deleteEntityService = Objects.requireNonNull(deleteEntityService, "deleteEntityService");
    this.timeseriesAspectService =
        Objects.requireNonNull(timeseriesAspectService, "timeseriesAspectService");
    this.graphService = Objects.requireNonNull(graphService, "graphService");
    this.clock = Objects.requireNonNull(clock, "clock");
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Hard-deletes {@code urn}. The caller has already authorized the delete. An absent entity is
   * reported {@code ALREADY_DELETED}, once its leftovers are removed. A delete that today's
   * validators reject throws before anything changes.
   *
   * @throws IllegalStateException when the delete fails. Part of it may be done; repeating the
   *     request is safe and finishes it.
   */
  @Nonnull
  public DeleteEntityReport delete(@Nonnull OperationContext actorContext, @Nonnull final Urn urn) {
    final long nowMillis = clock.millis();
    final Optional<DeleteCeiling> ceiling =
        entityService.captureDeleteCeiling(actorContext, urn, nowMillis);
    if (ceiling.isEmpty()) {
      // A retry of a request that failed after the key went: finish its cleanup. Idempotent, so an
      // entity that never existed costs only the no-op calls.
      return failingWithCause(
          urn,
          () ->
              new DeleteEntityReport(
                  urn.toString(),
                  ConditionalDeleteOutcome.ALREADY_DELETED,
                  0L,
                  removeNodeAndTimeseries(actorContext, urn, nowMillis),
                  0));
    }
    // Before the reference removal, so a rejected delete strips no references; deleteUrn validates
    // again because it is a public entry point of its own.
    entityService.validateHardDelete(actorContext, urn);

    return failingWithCause(urn, () -> deleteUpTo(actorContext, urn, ceiling.get()));
  }

  @Nonnull
  private static DeleteEntityReport failingWithCause(
      @Nonnull final Urn urn, @Nonnull final Supplier<DeleteEntityReport> step) {
    try {
      return step.get();
    } catch (RuntimeException e) {
      log.warn("Hard delete of {} failed", urn, e);
      throw new IllegalStateException(
          String.format("Hard delete of %s failed: %s", urn, failureDetail(e)), e);
    }
  }

  @Nonnull
  private DeleteEntityReport deleteUpTo(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final DeleteCeiling ceiling) {
    final int referencesRemoved =
        deleteEntityService.removeReferencesResumable(
            opContext, urn, null, DeleteCascadeListener.NOOP);

    final RollbackRunResult deleted = entityService.deleteUrn(opContext, urn, ceiling);
    final ConditionalDeleteOutcome outcome =
        Objects.requireNonNull(deleted.getConditionalDeleteOutcome(), "conditional delete outcome");
    final long rows =
        deleted.getRowsDeletedFromEntityDeletion() == null
            ? 0L
            : deleted.getRowsDeletedFromEntityDeletion();
    final long timeseriesRows =
        switch (outcome) {
          case DELETED -> removeNodeAndTimeseries(opContext, urn, ceiling.capturedAtMillis());
          case PARTIAL -> deleteTimeseriesUpTo(opContext, urn, ceiling.capturedAtMillis());
          // Deleted by someone else since the capture, or recreated: that delete (or the newer
          // entity) owns the rest.
          case ALREADY_DELETED -> 0L;
        };
    return new DeleteEntityReport(urn.toString(), outcome, rows, timeseriesRows, referencesRemoved);
  }

  /**
   * @return how many timeseries documents were deleted
   */
  private long removeNodeAndTimeseries(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn, final long upToMillis) {
    graphService.removeNodeReportingFailures(opContext, urn, true);
    return deleteTimeseriesUpTo(opContext, urn, upToMillis);
  }

  /**
   * Timeseries aspects carry no version, so the capture time is their ceiling: every timeseries
   * aspect's documents of {@code urn} with {@code timestampMillis} at or before it are deleted,
   * later ones survive. Idempotent.
   *
   * @return how many documents were deleted
   */
  private long deleteTimeseriesUpTo(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn, final long capturedAtMillis) {
    final Filter filter =
        QueryUtils.getFilterFromCriteria(
            List.of(
                CriterionUtils.buildCriterion("urn", Condition.EQUAL, urn.toString()),
                CriterionUtils.buildCriterion(
                    "timestampMillis",
                    Condition.LESS_THAN_OR_EQUAL_TO,
                    String.valueOf(capturedAtMillis))));
    long deleted = 0;
    for (AspectSpec aspectSpec :
        opContext.getEntityRegistry().getEntitySpec(urn.getEntityType()).getAspectSpecs()) {
      if (aspectSpec.isTimeseries()) {
        deleted +=
            timeseriesAspectService
                .deleteAspectValues(opContext, urn.getEntityType(), aspectSpec.getName(), filter)
                .getNumDocsDeleted();
      }
    }
    return deleted;
  }

  /** The top-level class and message only; cause chains stay in the server log. */
  @Nonnull
  private static String failureDetail(@Nonnull final RuntimeException e) {
    return e.getClass().getSimpleName() + ": " + (e.getMessage() == null ? "" : e.getMessage());
  }
}
