package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
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
 * aspect versions the entity has when the delete is requested. When the dispatcher takes it,
 * nothing is deleted here and the method returns at once (zero counts). When it declines, or there
 * is none, today's code runs here, unchanged: {@code featureFlags.reliableHardDelete} ({@link
 * ReliableHardDelete#isEnabled()}) picks the delete bounded by the versions or today's unbounded
 * {@code deleteUrn}. An entity that does not exist is not offered; today's code handles it.
 *
 * <p>The overloads that take the versions never offer: they run the delete here, bounded by the
 * versions given (never captured again) when the flag is on, and report an entity written to while
 * being deleted instead of throwing. They are for a process that runs a delete another process
 * captured.
 */
@Slf4j
public class HardDeleteService {
  private static final String ES_FIELD_TIMESTAMP = "timestampMillis";

  private final EntityService<?> entityService;
  private final DeleteEntityService deleteEntityService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final ReliableHardDelete reliableHardDelete;
  @Nullable private final HardDeleteDispatcher dispatcher;

  /**
   * @param dispatcher null runs every delete here
   */
  public HardDeleteService(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final DeleteEntityService deleteEntityService,
      @Nonnull final TimeseriesAspectService timeseriesAspectService,
      @Nonnull final ReliableHardDelete reliableHardDelete,
      @Nullable final HardDeleteDispatcher dispatcher) {
    this.entityService = Objects.requireNonNull(entityService, "entityService");
    this.deleteEntityService = Objects.requireNonNull(deleteEntityService, "deleteEntityService");
    this.timeseriesAspectService =
        Objects.requireNonNull(timeseriesAspectService, "timeseriesAspectService");
    this.reliableHardDelete = Objects.requireNonNull(reliableHardDelete, "reliableHardDelete");
    this.dispatcher = dispatcher;
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
   * when the request arrived.
   *
   * @return one result per urn, in order
   */
  @Nonnull
  public List<RollbackRunResult> deleteEntities(
      @Nonnull final OperationContext opContext, @Nonnull final Collection<Urn> urns) {
    final Map<Urn, Optional<DeleteCeiling>> versions = new LinkedHashMap<>();
    urns.forEach(urn -> versions.put(urn, capture(opContext, urn)));
    return urns.stream()
        .map(
            urn -> {
              final Optional<DeleteCeiling> entityVersions = versions.get(urn);
              if (entityVersions.isPresent()
                  && offered(opContext, HardDeleteRequest.entity(urn, entityVersions.get()))) {
                return nothingDeletedHere();
              }
              return deleteHere(opContext, urn, entityVersions);
            })
        .collect(Collectors.toList());
  }

  /**
   * Hard-deletes the entity, then hands back the delete of the references other entities hold to
   * it, for the caller to run where it runs that cleanup. The entity delete runs before this
   * returns and its failure is thrown, so references to an entity that stays are left alone. When
   * the entity does not exist, only its references are offered.
   *
   * @param runHere runs the reference cleanup when it stays in this process, the way the caller
   *     runs it today
   * @return the reference cleanup left to run; does nothing when it was taken elsewhere
   */
  @Nonnull
  public Runnable deleteEntityThenReferences(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull
          final Function<Supplier<DeleteReferencesResponse>, DeleteReferencesResponse> runHere) {
    final Optional<DeleteCeiling> versions = capture(opContext, urn);
    if (versions.isPresent()
        && offered(opContext, HardDeleteRequest.entityAndReferences(urn, versions.get()))) {
      return () -> {};
    }
    deleteHere(opContext, urn, versions);
    if (versions.isEmpty() && offered(opContext, HardDeleteRequest.references(urn))) {
      return () -> {};
    }
    return () -> runHere.apply(() -> deleteEntityService.deleteReferencesTo(opContext, urn, false));
  }

  /**
   * Hard-deletes the entity, then its values of the timeseries aspects {@code timeseriesAspects} in
   * the window, as the Rest.li {@code delete} action does; the caller has already authorized both.
   *
   * @param timeseriesContext the context the timeseries values are deleted under when the delete
   *     stays in this process, as the Rest.li action deletes them today
   * @return the rows and timeseries documents deleted, both 0 when the delete was taken elsewhere;
   *     the urn is left for the caller to set
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
    if (versions.isPresent()
        && offered(
            opContext,
            HardDeleteRequest.entityAndTimeseries(
                urn, versions.get(), timeseriesAspects, startTimeMillis, endTimeMillis))) {
      return new DeleteEntityResponse().setRows(0L).setTimeseriesRows(0L);
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

  /** Deletes the references other entities hold to the entity, as today's cleanup does. */
  @Nonnull
  public DeleteReferencesResponse deleteReferences(
      @Nonnull final OperationContext opContext, @Nonnull final Urn urn) {
    return deleteReferences(opContext, urn, Supplier::get);
  }

  /**
   * {@link #deleteReferences(OperationContext, Urn)}, offered once either way.
   *
   * @param runHere runs the cleanup when it stays in this process, the way the caller runs it today
   * @return what the cleanup did; empty when it was taken elsewhere
   */
  @Nonnull
  public DeleteReferencesResponse deleteReferences(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull
          final Function<Supplier<DeleteReferencesResponse>, DeleteReferencesResponse> runHere) {
    if (offered(opContext, HardDeleteRequest.references(urn))) {
      return new DeleteReferencesResponse().setTotal(0).setRelatedAspects(new RelatedAspectArray());
    }
    return runHere.apply(() -> deleteEntityService.deleteReferencesTo(opContext, urn, false));
  }

  /**
   * Hard-deletes the entity here, never offering it: bounded by {@code versions} when {@code
   * reliableHardDelete} is on, else today's unbounded {@code deleteUrn}. An entity that is already
   * gone is not deleted again.
   *
   * @param versions the aspect versions captured when the delete was requested
   * @return false when the bounded delete left the entity because it was written to while being
   *     deleted; nothing is thrown for it
   */
  public boolean deleteEntity(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final DeleteCeiling versions) {
    if (!reliableHardDelete.isEnabled()) {
      entityService.deleteUrn(opContext, urn);
      return true;
    }
    // Only checks the entity is still there; the delete is bounded by the versions given.
    if (reliableHardDelete.capture(opContext, urn).isEmpty()) {
      return true;
    }
    return reliableHardDelete.deleteBounded(opContext, urn, versions).outcome()
        != ConditionalDeleteOutcome.PARTIAL;
  }

  /**
   * {@link #deleteEntity(OperationContext, Urn, DeleteCeiling)}, then the references other entities
   * hold to it, never offering either. The references are left alone when the entity stays.
   *
   * <p>The entities holding a graph reference are read before the entity is deleted. Processing the
   * delete of its key removes every graph edge of the entity, and a process whose first graph read
   * comes after that (a cold search client takes about a second) would find no references to
   * remove. Every other kind of reference is found as today.
   *
   * <p>Known limit: run again after the entity is gone (the first run failed after its delete), the
   * graph no longer has those edges, so no graph references are found and they stay, as when a
   * cleanup reads the graph after the edges are removed today.
   *
   * @return false when the entity stays (and the references were left alone)
   */
  public boolean deleteEntityThenReferences(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final DeleteCeiling versions) {
    final List<RelatedEntities> graphReferrers =
        deleteEntityService.getGraphReferrers(opContext, urn);
    if (!deleteEntity(opContext, urn, versions)) {
      return false;
    }
    deleteEntityService.deleteReferencesTo(opContext, urn, graphReferrers);
    return true;
  }

  /**
   * {@link #deleteEntity(OperationContext, Urn, DeleteCeiling)}, then the entity's timeseries
   * values as {@link #deleteEntityAndTimeseries(OperationContext, OperationContext, Urn, List,
   * Long, Long)} deletes them, never offering either. The values are left alone when the entity
   * stays.
   *
   * @return false when the entity stays (and the timeseries values were left alone)
   */
  public boolean deleteEntityAndTimeseries(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn urn,
      @Nonnull final DeleteCeiling versions,
      @Nonnull final List<String> timeseriesAspects,
      @Nullable final Long startTimeMillis,
      @Nullable final Long endTimeMillis) {
    if (!deleteEntity(opContext, urn, versions)) {
      return false;
    }
    final long numTimeseriesDocsDeleted =
        deleteTimeseriesAspects(opContext, urn, timeseriesAspects, startTimeMillis, endTimeMillis);
    log.info("Total number of timeseries aspect docs deleted: {}", numTimeseriesDocsDeleted);
    return true;
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

    // Construct the filter.
    List<Criterion> criteria = new ArrayList<>();
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
    final Filter filter = QueryUtils.getFilterFromCriteria(criteria);

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

  /** What {@code deleteUrn} returns when it deleted nothing. */
  @Nonnull
  private static RollbackRunResult nothingDeletedHere() {
    return new RollbackRunResult(List.of(), 0, List.of());
  }
}
