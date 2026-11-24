package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.service.HardDeleteDispatcher;
import com.linkedin.metadata.service.HardDeleteRequest;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * The hard deletes the entity client, the Rest.li entity resource and the OpenAPI entity deletes
 * run, in one place. The caller has already authorized each one.
 *
 * <p>Each delete is offered once to the {@link HardDeleteDispatcher}, when there is one, with the
 * aspect versions the entity has when the delete is requested. When the dispatcher takes it (it
 * hands the delete to another process, or runs it itself), nothing more is deleted here and the
 * method returns zero counts, except the two Rest.li-only methods: they count before the offer
 * (only when there is a dispatcher), report those counts, and set {@code queued} only when the
 * delete was queued rather than already done ({@link HardDeleteDispatcher#offer}). When it
 * declines, or there is none, today's code runs here, unchanged: {@code
 * featureFlags.reliableHardDelete} ({@link ReliableHardDelete#isEnabled()}) picks the delete
 * bounded by the versions or today's unbounded {@code deleteUrn}. An entity that does not exist is
 * not offered; today's code handles it.
 */
@Slf4j
public class HardDeleteService {
  /**
   * The references still to remove after {@link #deleteReferencesThenEntity(OperationContext, Urn,
   * Function)} has deleted the entity, or decided not to. {@link #choice()} is known then. {@link
   * Choice#PENDING} is an entity that was already gone. {@link HardDeleteService#HANDED_OFF} is
   * {@link Choice#HANDED_OFF}: nothing was deleted here and nothing is left to run.
   */
  public static final class ReferenceCleanup implements Runnable {
    /** What an in-process entity-and-references delete did about the referrers. */
    public enum Choice {
      /**
       * The entity was already gone. {@link #run()} only cleans its references, or does nothing
       * when those were taken elsewhere.
       */
      PENDING,
      /**
       * Incoming graph edges were at most the limit: they were removed, then the entity. {@link
       * #run()} removes the references that are not graph edges.
       */
      REFERENCES_THEN_ENTITY,
      /**
       * The limit is 0, the operator's opt-in to leaving graph references alone: only the entity
       * was hard-deleted, and the key delete dropped its graph edges. {@link #run()} still removes
       * the references that are not graph edges. A delete over a limit above 0 is refused instead.
       */
      ENTITY_ONLY,
      /** The delete was taken by another process, which also cleans the references. */
      HANDED_OFF
    }

    private final Choice choice;
    private final Runnable body;

    private ReferenceCleanup(final Choice choice, final Runnable body) {
      this.choice = choice;
      this.body = body;
    }

    /** An absent entity. {@link #choice()} stays {@link Choice#PENDING}. */
    static ReferenceCleanup pending(final Runnable work) {
      return new ReferenceCleanup(Choice.PENDING, work);
    }

    @Nonnull
    public Choice choice() {
      return choice;
    }

    @Override
    public void run() {
      body.run();
    }
  }

  /**
   * What {@link #deleteReferencesThenEntity(OperationContext, Urn, Function)} returns when the
   * entity delete was taken elsewhere: nothing was deleted here and nothing is left to run.
   */
  public static final ReferenceCleanup HANDED_OFF =
      new ReferenceCleanup(ReferenceCleanup.Choice.HANDED_OFF, () -> {});

  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";

  private final EntityService<?> entityService;
  private final DeleteEntityService deleteEntityService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final ReliableHardDelete reliableHardDelete;
  @Nullable private final HardDeleteDispatcher dispatcher;
  private final int graphReferenceCleanupLimit;

  /**
   * @param dispatcher null runs every delete here
   * @param graphReferenceCleanupLimit most incoming graph edges removed before the entity when the
   *     delete runs here. Above it, {@link #deleteReferencesThenEntity} refuses the delete and
   *     removes nothing. {@code 0} never removes graph references here and deletes the entity only.
   *     Negative is rejected.
   */
  public HardDeleteService(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final DeleteEntityService deleteEntityService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final ReliableHardDelete reliableHardDelete,
      @Nullable final HardDeleteDispatcher dispatcher,
      final int graphReferenceCleanupLimit) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
    this.deleteEntityService = Objects.requireNonNull(deleteEntityService, "deleteEntityService");
    this.timeseriesAspectService =
        Objects.requireNonNull(timeseriesAspectService, "timeseriesAspectService");
    this.reliableHardDelete = Objects.requireNonNull(reliableHardDelete, "reliableHardDelete");
    this.dispatcher = dispatcher;
    if (graphReferenceCleanupLimit < 0) {
      throw new IllegalArgumentException(
          "graph-reference-cleanup-limit must be at least 0, was " + graphReferenceCleanupLimit);
    }
    this.graphReferenceCleanupLimit = graphReferenceCleanupLimit;
  }

  /**
   * Hard-deletes the entity.
   *
   * @return what {@code deleteUrn} returned; empty when the delete was taken elsewhere
   * @throws IllegalStateException when the bounded delete left the entity, as {@link
   *     ReliableHardDelete#delete(OperationContext, Urn)} does
   */
  @Nonnull
  public RollbackRunResult deleteEntity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    return deleteEntities(opContext, List.of(urn)).get(0);
  }

  /**
   * Hard-deletes each entity, in order, as {@link #deleteEntity(OperationContext, Urn)} does. The
   * versions of all of them are captured before the first is deleted, so each bound is the state
   * when the request arrived. With more than one urn each delete is offered as one of many ({@link
   * HardDeleteDispatcher#dispatchOneOfMany}).
   *
   * @return one result per urn, in order
   */
  @Nonnull
  public List<RollbackRunResult> deleteEntities(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<Urn> urns) {
    final Map<Urn, Optional<DeleteCeiling>> versions = new LinkedHashMap<>();
    urns.forEach(urn -> versions.put(urn, capture(opContext, urn)));
    final boolean oneOfMany = urns.size() > 1;
    return urns.stream()
        .map(
            urn -> {
              final Optional<DeleteCeiling> entityVersions = versions.get(urn);
              if (entityVersions.isPresent()
                  && offered(
                      opContext, HardDeleteRequest.entity(urn, entityVersions.get()), oneOfMany)) {
                return nothingDeletedHere();
              }
              return deleteHere(opContext, urn, entityVersions);
            })
        .collect(Collectors.toList());
  }

  /**
   * Removes the graph references other entities hold to the entity, then hard-deletes the entity,
   * and hands back the removal of the references that are not graph edges for the caller to run
   * where it runs that cleanup. The entity is deleted after its graph references: the key delete
   * drops the graph edges, so a later read would find none.
   *
   * <p>Nothing is removed until the delete validators and guards have accepted the key delete
   * ({@link EntityService#validateKeyDelete}), so a refused delete leaves the references in place.
   * The count of incoming graph edges, their removal and the entity delete then run under {@code
   * runHere}, so a transient failure is retried as the caller retries reference cleanup. At most
   * {@code graph-reference-cleanup-limit} edges are removed one by one before the entity is
   * deleted. Above the limit the delete is refused with a {@link
   * GraphReferenceLimitExceededException} before anything is removed: deleting the entity alone
   * would leave the referrers pointing at nothing. A limit of 0 skips the count and deletes the
   * entity only ({@link ReferenceCleanup.Choice#ENTITY_ONLY}). The count is one {@code runHere}
   * step and the cleanup and delete another, so a retry of the cleanup does not count again, and
   * the refusal is never retried. A failure of the validation, the count, the page loop or the
   * delete is thrown here and leaves the entity in place. When the entity does not exist, only its
   * references are offered, and the cleanup only cleans those, without a count.
   *
   * @param runHere runs a step that stays in this process, the way the caller runs reference
   *     cleanup today
   * @return the references left to remove, whose {@code run} removes the ones that are not graph
   *     edges; {@link #HANDED_OFF} when the delete was queued elsewhere and has not run yet, a
   *     no-op {@link ReferenceCleanup.Choice#REFERENCES_THEN_ENTITY} when the dispatcher already
   *     ran it, and a no-op when only the references of an absent entity were taken
   * @throws GraphReferenceLimitExceededException when more graph edges point at the entity than the
   *     limit allows
   */
  @Nonnull
  public ReferenceCleanup deleteReferencesThenEntity(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull
          final Function<Supplier<DeleteReferencesResponse>, DeleteReferencesResponse> runHere) {
    final Optional<DeleteCeiling> versions = capture(opContext, urn);
    if (versions.isPresent() && dispatcher != null) {
      final HardDeleteDispatcher.Outcome outcome =
          dispatcher
              .offer(opContext, HardDeleteRequest.entityAndReferences(urn, versions.get()))
              .outcome();
      if (outcome == HardDeleteDispatcher.Outcome.QUEUED) {
        return HANDED_OFF;
      }
      if (outcome == HardDeleteDispatcher.Outcome.DONE) {
        // The dispatcher deleted the entity and its references before it returned.
        return new ReferenceCleanup(ReferenceCleanup.Choice.REFERENCES_THEN_ENTITY, () -> {});
      }
    }
    if (versions.isEmpty() && offered(opContext, HardDeleteRequest.references(urn))) {
      // The entity was already gone here; only its references were taken.
      return ReferenceCleanup.pending(() -> {});
    }
    // capture() is empty both when the entity is gone and when nothing needed the versions. Only
    // the first is an absent entity: with no dispatcher and the bounded delete off, the entity was
    // not read and is deleted unbounded below. The service cannot tell an absent entity from a live
    // one there, so stale edges to an absent entity above the limit are refused like any other.
    if (versions.isEmpty() && (dispatcher != null || reliableHardDelete.isEnabled())) {
      return ReferenceCleanup.pending(
          () -> runHere.apply(() -> deleteEntityService.deleteReferencesTo(opContext, urn, false)));
    }

    // A delete that validation refuses must not have removed a reference first.
    validateDelete(opContext, urn);

    // A limit of 0 is the operator's explicit opt-in to leaving graph references to the key delete,
    // so there is nothing to count against it.
    final boolean cleanGraphReferences = graphReferenceCleanupLimit > 0;
    if (cleanGraphReferences) {
      // The count has its own retried step: the refusal below is deterministic, and retrying it
      // would only repeat the same count.
      final AtomicInteger referrers = new AtomicInteger();
      runHere.apply(
          () -> {
            referrers.set(deleteEntityService.countGraphReferrers(opContext, urn));
            return noReferencesDeleted();
          });
      if (referrers.get() > graphReferenceCleanupLimit) {
        // Nothing has been removed yet. Deleting the entity alone would drop its edges and leave
        // the referrers pointing at nothing, and walking this many of them here is what the limit
        // refuses.
        throw new GraphReferenceLimitExceededException(
            urn, referrers.get(), graphReferenceCleanupLimit);
      }
    } else {
      log.warn(
          "Graph reference cleanup is disabled (limit 0): hard-deleting {} only, which leaves any"
              + " referrers dangling.",
          urn);
    }
    runHere.apply(
        () -> {
          if (cleanGraphReferences) {
            deleteEntityService.deleteGraphReferences(opContext, urn);
          }
          deleteHere(opContext, urn, versions);
          return noReferencesDeleted();
        });
    final ReferenceCleanup.Choice choice =
        cleanGraphReferences
            ? ReferenceCleanup.Choice.REFERENCES_THEN_ENTITY
            : ReferenceCleanup.Choice.ENTITY_ONLY;
    // The graph edges are gone with the entity (removed above, or dropped by the key delete): an
    // empty list leaves only the references that are not graph edges.
    return new ReferenceCleanup(
        choice,
        () ->
            runHere.apply(
                () -> deleteEntityService.deleteReferencesTo(opContext, urn, false, List.of())));
  }

  /**
   * Runs the checks a hard delete of {@code urn} makes before it writes, so a caller that removes
   * something else first (references, timeseries values) does not remove it for a delete that is
   * then refused. See {@link EntityService#validateKeyDelete}.
   */
  public void validateDelete(@Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    entityService.validateKeyDelete(opContext, urn);
  }

  /**
   * Hard-deletes the entity, then its values of the timeseries aspects {@code timeseriesAspects} in
   * the window, as the Rest.li {@code delete} action does; the caller has already authorized both.
   *
   * @param timeseriesContext the context the timeseries values are deleted under when the delete
   *     stays in this process, as the Rest.li action deletes them today
   * @return the rows and timeseries documents deleted. When a dispatcher takes the delete, the
   *     counts are what existed just before it was offered ({@code -1} where a count failed), and
   *     {@code queued} is true only when it was queued rather than already done; the urn is left
   *     for the caller to set
   */
  @Nonnull
  public DeleteEntityResponse deleteEntityAndTimeseries(
      @Nonnull final OperationContext opContext,
      @Nonnull final OperationContext timeseriesContext,
      @Nonnull final Urn urn,
      @Nonnull final List<String> timeseriesAspects,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    final Optional<DeleteCeiling> versions = capture(opContext, urn);
    if (versions.isPresent() && dispatcher != null) {
      // Counted before the offer: a delete the dispatcher runs itself is gone once it returns.
      final long rows =
          countOrUnknown(() -> entityService.countUrnRows(opContext, urn), "rows", urn);
      final long timeseriesRows =
          countOrUnknown(
              () ->
                  countTimeseries(
                      timeseriesContext, urn, timeseriesAspects, startTimeMillis, endTimeMillis),
              "timeseries rows",
              urn);
      final HardDeleteDispatcher.Outcome outcome =
          dispatcher
              .offer(
                  opContext,
                  HardDeleteRequest.entityAndTimeseries(
                      urn, versions.get(), timeseriesAspects, startTimeMillis, endTimeMillis))
              .outcome();
      if (outcome != HardDeleteDispatcher.Outcome.DECLINED) {
        final DeleteEntityResponse taken =
            new DeleteEntityResponse().setRows(rows).setTimeseriesRows(timeseriesRows);
        return outcome == HardDeleteDispatcher.Outcome.QUEUED ? taken.setQueued(true) : taken;
      }
    }

    final DeleteEntityResponse response = new DeleteEntityResponse();
    if (reliableHardDelete.isEnabled()) {
      response.setRows(reliableHardDelete.delete(opContext, urn, versions).rowsDeleted());
    } else {
      RollbackRunResult result = entityService.deleteUrn(opContext, urn);
      Integer rows = result.getRowsDeletedFromEntityDeletion();
      response.setRows(rows != null ? rows.longValue() : 0L);
    }
    final long numTimeseriesDocsDeleted =
        deleteTimeseriesAspects(
            timeseriesContext, urn, timeseriesAspects, startTimeMillis, endTimeMillis);
    log.info("Total number of timeseries aspect docs deleted: {}", numTimeseriesDocsDeleted);
    return response.setTimeseriesRows(numTimeseriesDocsDeleted);
  }

  /**
   * Deletes the references other entities hold to the entity, as today's cleanup does. When a
   * dispatcher takes the cleanup, the response is the dry run's count of what was there just before
   * it was offered ({@code total} -1 when the dry run failed), and {@code queued} is true only when
   * it was queued rather than already done.
   */
  @Nonnull
  public DeleteReferencesResponse deleteReferences(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    if (dispatcher == null) {
      return deleteEntityService.deleteReferencesTo(opContext, urn, false);
    }
    // Counted before the offer: a cleanup the dispatcher runs itself is gone once it returns.
    final DeleteReferencesResponse counted = countReferences(opContext, urn);
    final HardDeleteDispatcher.Offer offer =
        dispatcher.offer(opContext, HardDeleteRequest.references(urn));
    if (offer.outcome() == HardDeleteDispatcher.Outcome.DECLINED) {
      return deleteEntityService.deleteReferencesTo(opContext, urn, false);
    }
    // The dry run reads the graph only. After an entity delete dropped the edges, the referrers it
    // kept are only in its list, which the dispatcher counted while routing.
    if (offer.references() != null && offer.references() > counted.getTotal()) {
      counted.setTotal(offer.references());
    }
    return offer.outcome() == HardDeleteDispatcher.Outcome.QUEUED
        ? counted.setQueued(true)
        : counted;
  }

  /**
   * Deletes the values of {@code aspectsToDelete} associated with {@code urn} between {@code
   * startTimeMillis} and {@code endTimeMillis}; the caller has already authorized it.
   *
   * @param startTimeMillis null deletes from the oldest value
   * @param endTimeMillis null deletes up to the most recent value
   * @return the total number of documents deleted
   */
  public long deleteTimeseriesAspects(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final List<String> aspectsToDelete,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    if (aspectsToDelete.isEmpty()) {
      return 0L;
    }

    long totalNumberOfDocsDeleted = 0;

    final Filter filter = timeseriesFilter(urn, startTimeMillis, endTimeMillis);

    // Delete all the timeseries aspects by the filter.
    final String entityType = urn.getEntityType();
    for (final String aspect : aspectsToDelete) {
      DeleteAspectValuesResult result =
          timeseriesAspectService.deleteAspectValues(opContext, entityType, aspect, filter);
      totalNumberOfDocsDeleted += result.getNumDocsDeleted();

      log.debug(
          "Number of timeseries docs deleted for entity:{}, aspect:{}, urn:{}, startTime:{}, endTime:{}={}",
          entityType,
          aspect,
          urn,
          startTimeMillis,
          endTimeMillis,
          result.getNumDocsDeleted());
    }
    return totalNumberOfDocsDeleted;
  }

  /** The documents {@link #deleteTimeseriesAspects} deletes, matched by the same filter. */
  private long countTimeseries(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final List<String> aspects,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    if (aspects.isEmpty()) {
      return 0L;
    }
    final Filter filter = timeseriesFilter(urn, startTimeMillis, endTimeMillis);
    long total = 0;
    for (final String aspect : aspects) {
      total +=
          timeseriesAspectService.countByFilter(opContext, urn.getEntityType(), aspect, filter);
    }
    return total;
  }

  /**
   * The dry run of the references cleanup, taken before the cleanup is offered. A dry run that
   * fails is {@code -1}, not a failed request: the count is only reported.
   */
  @Nonnull
  private DeleteReferencesResponse countReferences(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    try {
      return deleteEntityService.deleteReferencesTo(opContext, urn, true);
    } catch (RuntimeException e) {
      log.warn(
          "Could not count the references to {} before offering its cleanup; reporting -1", urn, e);
      return new DeleteReferencesResponse()
          .setTotal(-1)
          .setRelatedAspects(new RelatedAspectArray());
    }
  }

  /** The count, or -1 when it fails or the store cannot count: the count is only reported. */
  private static long countOrUnknown(
      @Nonnull final Supplier<Number> count, @Nonnull final String what, @Nonnull final Urn urn) {
    try {
      return count.get().longValue();
    } catch (RuntimeException e) {
      log.warn("Could not count {} for {} for a queued delete; reporting -1", what, urn, e);
      return -1L;
    }
  }

  /**
   * The timeseries values of {@code urn} between {@code startTimeMillis} and {@code endTimeMillis}
   * (null ends are open), as {@link #deleteTimeseriesAspects} deletes them; a caller counts with
   * the same filter.
   */
  @Nonnull
  public static Filter timeseriesFilter(
      @Nonnull final Urn urn,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    final List<Criterion> criteria = new ArrayList<>();
    criteria.add(CriterionUtils.buildCriterion("urn", Condition.EQUAL, urn.toString()));
    if (startTimeMillis != null) {
      criteria.add(
          CriterionUtils.buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.GREATER_THAN_OR_EQUAL_TO, startTimeMillis.toString()));
    }
    if (endTimeMillis != null) {
      criteria.add(
          CriterionUtils.buildCriterion(
              ES_FIELD_TIMESTAMP, Condition.LESS_THAN_OR_EQUAL_TO, endTimeMillis.toString()));
    }
    return QueryUtils.getFilterFromCriteria(criteria);
  }

  /**
   * The versions the entity has now, when the dispatcher or the bounded delete needs them; empty
   * when the entity does not exist, and without a read when neither needs them.
   */
  @Nonnull
  private Optional<DeleteCeiling> capture(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    return dispatcher != null || reliableHardDelete.isEnabled()
        ? reliableHardDelete.capture(opContext, urn)
        : Optional.empty();
  }

  /** Today's entity delete. */
  @Nonnull
  private RollbackRunResult deleteHere(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final Optional<DeleteCeiling> versions) {
    return reliableHardDelete.isEnabled()
        ? reliableHardDelete.delete(opContext, urn, versions).rollbackRunResult()
        : entityService.deleteUrn(opContext, urn);
  }

  private boolean offered(
      @Nonnull final OperationContext opContext, @Nonnull final HardDeleteRequest request) {
    return dispatcher != null && dispatcher.dispatch(opContext, request);
  }

  private boolean offered(
      @Nonnull final OperationContext opContext,
      @Nonnull final HardDeleteRequest request,
      final boolean oneOfMany) {
    if (!oneOfMany) {
      return offered(opContext, request);
    }
    return dispatcher != null && dispatcher.dispatchOneOfMany(opContext, request);
  }

  private static DeleteReferencesResponse noReferencesDeleted() {
    return new DeleteReferencesResponse().setTotal(0).setRelatedAspects(new RelatedAspectArray());
  }

  /** What {@code deleteUrn} returns when it deleted nothing. */
  @Nonnull
  private static RollbackRunResult nothingDeletedHere() {
    return new RollbackRunResult(List.of(), 0, List.of());
  }
}
