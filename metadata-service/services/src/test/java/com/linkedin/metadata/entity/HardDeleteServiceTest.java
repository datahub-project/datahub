package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
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
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Each delete: no dispatcher runs today's code; taken runs nothing here; declined runs today's code
 * after one offer; the {@code reliableHardDelete} flag never decides the offer. The overloads that
 * take versions never offer, and the flag picks bounded or unbounded.
 */
public class HardDeleteServiceTest {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn OTHER =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.other,PROD)");
  private static final DeleteCeiling CAPTURED =
      new DeleteCeiling(Map.of("datasetKey", 1L, "datasetProperties", 3L), 7L);
  private static final DeleteCeiling GIVEN =
      new DeleteCeiling(Map.of("datasetKey", 1L, "datasetProperties", 2L), 7L);
  private static final List<String> ASPECTS = List.of("datasetProfile");
  private static final long START = 10L;
  private static final long END = 20L;
  private static final List<RelatedEntities> REFERRERS =
      List.of(
          new RelatedEntities(
              "DownstreamOf",
              OTHER.toString(),
              URN.toString(),
              RelationshipDirection.INCOMING,
              null));

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityService<?> entityService;
  private DeleteEntityService deleteEntityService;
  private TimeseriesAspectService timeseriesAspectService;
  private HardDeleteDispatcher dispatcher;
  private DeleteReferencesResponse referencesDeleted;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    dispatcher = mock(HardDeleteDispatcher.class);
    referencesDeleted =
        new DeleteReferencesResponse().setTotal(2).setRelatedAspects(new RelatedAspectArray());
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CAPTURED));
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(keyDeleted(URN, 5));
    when(entityService.deleteUrn(any(), eq(URN))).thenReturn(keyDeleted(URN, 6));
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), anyBoolean()))
        .thenReturn(referencesDeleted);
    when(deleteEntityService.getGraphReferrers(any(), eq(URN))).thenReturn(REFERRERS);
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), anyList()))
        .thenReturn(referencesDeleted);
    when(timeseriesAspectService.deleteAspectValues(any(), anyString(), anyString(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(4L));
  }

  // ---- deleteEntity(op, urn)

  @Test
  public void deleteEntityWithoutDispatcherAndFlagOnIsBoundedByTheCapture() {
    service(true, null).deleteEntity(opContext, URN);

    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verify(entityService, never()).deleteUrn(any(), eq(URN));
  }

  @Test
  public void deleteEntityWithoutDispatcherAndFlagOffIsUnboundedWithoutACapture() {
    service(false, null).deleteEntity(opContext, URN);

    verify(entityService).deleteUrn(any(), eq(URN));
    verify(entityService, never()).captureDeleteCeiling(any(), any());
  }

  @Test
  public void deleteEntityTakenRunsNothingHere() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    final RollbackRunResult result = service(true, dispatcher).deleteEntity(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(URN, CAPTURED));
    assertTrue(result.getRollbackResults().isEmpty());
    assertEquals(result.getRowsDeletedFromEntityDeletion(), Integer.valueOf(0));
    verifyNoDelete();
  }

  @Test
  public void deleteEntityDeclinedRunsTodaysCodeAfterOneOffer() {
    service(true, dispatcher).deleteEntity(opContext, URN);

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(entityService, times(1)).captureDeleteCeiling(any(), eq(URN));
    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
  }

  @Test
  public void deleteEntityWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(false, dispatcher).deleteEntity(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(URN, CAPTURED));
    verifyNoDelete();
  }

  @Test
  public void deleteEntityOfAnAbsentEntityIsNotOfferedAndRunsTodaysCode() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());

    service(true, dispatcher).deleteEntity(opContext, URN);

    verifyNoInteractions(dispatcher);
    verify(entityService, never()).deleteUrn(any(), eq(URN), any(DeleteCeiling.class));
  }

  @Test
  public void deleteEntityPartialThrowsTodaysError() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    expectThrows(
        IllegalStateException.class, () -> service(true, dispatcher).deleteEntity(opContext, URN));
  }

  /** Every entity's versions are captured before the first is deleted. */
  @Test
  public void deleteEntitiesCapturesAllBeforeDeletingAny() {
    final DeleteCeiling other = new DeleteCeiling(Map.of("datasetKey", 1L), 9L);
    when(entityService.captureDeleteCeiling(any(), eq(OTHER))).thenReturn(Optional.of(other));
    when(entityService.deleteUrn(any(), eq(OTHER), any(DeleteCeiling.class)))
        .thenReturn(keyDeleted(OTHER, 1));

    final List<RollbackRunResult> results =
        service(true, dispatcher).deleteEntities(opContext, List.of(URN, OTHER));

    assertEquals(results.size(), 2);
    final InOrder inOrder = inOrder(entityService, dispatcher);
    inOrder.verify(entityService).captureDeleteCeiling(any(), eq(URN));
    inOrder.verify(entityService).captureDeleteCeiling(any(), eq(OTHER));
    inOrder.verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(URN, CAPTURED));
    inOrder.verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    inOrder.verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(OTHER, other));
    inOrder.verify(entityService).deleteUrn(any(), eq(OTHER), eq(other));
  }

  // ---- deleteEntityThenReferences(op, urn, runHere)

  @Test
  public void deleteEntityThenReferencesWithoutDispatcherDeletesNowAndHandsBackTheReferences() {
    final Runnable references =
        service(true, null).deleteEntityThenReferences(opContext, URN, Supplier::get);

    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verifyNoInteractions(deleteEntityService);
    references.run();
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    verifyGraphReferrersNotReadAhead();
  }

  @Test
  public void deleteEntityThenReferencesWithTheFlagOffDeletesUnbounded() {
    service(false, null).deleteEntityThenReferences(opContext, URN, Supplier::get).run();

    verify(entityService).deleteUrn(any(), eq(URN));
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    verifyGraphReferrersNotReadAhead();
  }

  @Test
  public void deleteEntityThenReferencesTakenRunsNothingHere() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(true, dispatcher).deleteEntityThenReferences(opContext, URN, Supplier::get).run();

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entityAndReferences(URN, CAPTURED));
    verifyNoDelete();
    verifyNoInteractions(deleteEntityService);
  }

  /** Declined: both run here; the references go through the caller's runner, never re-offered. */
  @Test
  public void deleteEntityThenReferencesDeclinedRunsBothHereAfterOneOffer() {
    final List<String> ranBy = new ArrayList<>();

    service(true, dispatcher)
        .deleteEntityThenReferences(
            opContext,
            URN,
            cleanup -> {
              ranBy.add("caller");
              return cleanup.get();
            })
        .run();

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entityAndReferences(URN, CAPTURED));
    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    verifyGraphReferrersNotReadAhead();
    assertEquals(ranBy, List.of("caller"));
  }

  @Test
  public void deleteEntityThenReferencesWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(false, dispatcher).deleteEntityThenReferences(opContext, URN, Supplier::get).run();

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entityAndReferences(URN, CAPTURED));
    verifyNoDelete();
  }

  /** PARTIAL throws before the references are handed out, as today. */
  @Test
  public void deleteEntityThenReferencesPartialLeavesTheReferences() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    expectThrows(
        IllegalStateException.class,
        () -> service(true, dispatcher).deleteEntityThenReferences(opContext, URN, Supplier::get));
    verifyNoInteractions(deleteEntityService);
  }

  /** An absent entity is handled here as today; its references are still offered. */
  @Test
  public void deleteEntityThenReferencesOfAnAbsentEntityOffersOnlyTheReferences() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());
    when(dispatcher.dispatch(opContext, HardDeleteRequest.references(URN))).thenReturn(true);

    service(true, dispatcher).deleteEntityThenReferences(opContext, URN, Supplier::get).run();

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(entityService, never()).deleteUrn(any(), eq(URN), any(DeleteCeiling.class));
    verifyNoInteractions(deleteEntityService);
  }

  // ---- deleteEntityAndTimeseries(op, urn, aspects, start, end)

  @Test
  public void deleteEntityAndTimeseriesWithoutDispatcherRunsTodaysCode() {
    final DeleteEntityResponse response =
        service(true, null)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
    assertEquals(response.getRows(), Long.valueOf(5L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(4L));
  }

  @Test
  public void deleteEntityAndTimeseriesWithTheFlagOffIsUnbounded() {
    final DeleteEntityResponse response =
        service(false, null)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(entityService).deleteUrn(any(), eq(URN));
    assertEquals(response.getRows(), Long.valueOf(6L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(4L));
  }

  @Test
  public void deleteEntityAndTimeseriesTakenRunsNothingHere() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(dispatcher)
        .dispatch(
            opContext, HardDeleteRequest.entityAndTimeseries(URN, CAPTURED, ASPECTS, START, END));
    assertEquals(response.getRows(), Long.valueOf(0L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(0L));
    verifyNoDelete();
    verifyNoInteractions(timeseriesAspectService);
  }

  @Test
  public void deleteEntityAndTimeseriesDeclinedRunsTodaysCodeAfterOneOffer() {
    service(true, dispatcher)
        .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
  }

  @Test
  public void deleteEntityAndTimeseriesWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(false, dispatcher)
        .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(dispatcher, times(1)).dispatch(any(), any());
    verifyNoDelete();
  }

  /** PARTIAL throws before the timeseries values are deleted, as today. */
  @Test
  public void deleteEntityAndTimeseriesPartialLeavesTheTimeseries() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    expectThrows(
        IllegalStateException.class,
        () ->
            service(true, dispatcher)
                .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END));
    verifyNoInteractions(timeseriesAspectService);
  }

  // ---- deleteReferences(op, urn)

  @Test
  public void deleteReferencesWithoutDispatcherRunsTodaysCode() {
    assertSame(service(true, null).deleteReferences(opContext, URN), referencesDeleted);
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    verifyGraphReferrersNotReadAhead();
  }

  @Test
  public void deleteReferencesTakenRunsNothingHereAndReportsNothing() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.references(URN));
    assertEquals(response.getTotal(), Integer.valueOf(0));
    assertTrue(response.getRelatedAspects().isEmpty());
    verifyNoInteractions(deleteEntityService);
  }

  @Test
  public void deleteReferencesDeclinedRunsTodaysCodeThroughTheCallersRunnerAfterOneOffer() {
    final List<String> ranBy = new ArrayList<>();

    service(true, dispatcher)
        .deleteReferences(
            opContext,
            URN,
            cleanup -> {
              ranBy.add("caller");
              return cleanup.get();
            });

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    assertEquals(ranBy, List.of("caller"));
  }

  @Test
  public void deleteReferencesWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(false, dispatcher).deleteReferences(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.references(URN));
    verifyNoInteractions(deleteEntityService);
  }

  // ---- the overloads that take versions: never offered

  /** Bounded by the versions given, though the entity has other versions now. */
  @Test
  public void deleteEntityWithVersionsAndTheFlagOnIsBoundedByThemAndNeverOffered() {
    assertTrue(service(true, dispatcher).deleteEntity(opContext, URN, GIVEN));

    verify(entityService).deleteUrn(any(), eq(URN), eq(GIVEN));
    verify(entityService, never()).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void deleteEntityWithVersionsAndTheFlagOffIsUnbounded() {
    assertTrue(service(false, dispatcher).deleteEntity(opContext, URN, GIVEN));

    verify(entityService).deleteUrn(any(), eq(URN));
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void deleteEntityWithVersionsReportsPartialWithoutThrowing() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    assertFalse(service(true, null).deleteEntity(opContext, URN, GIVEN));
  }

  /** A repeat after the entity is gone deletes nothing again. */
  @Test
  public void deleteEntityWithVersionsOfAGoneEntityDeletesNothing() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());

    assertTrue(service(true, null).deleteEntity(opContext, URN, GIVEN));

    verify(entityService, never()).deleteUrn(any(), eq(URN), any(DeleteCeiling.class));
  }

  /**
   * The entity is deleted, then the graph referrers the caller read before it are cleaned; the
   * graph is not read again (once the key delete is processed it has no edges of the entity).
   */
  @Test
  public void deleteEntityThenReferencesWithVersionsDeletesThenCleansTheGivenReferrers() {
    assertTrue(
        service(true, dispatcher).deleteEntityThenReferences(opContext, URN, GIVEN, REFERRERS));

    final InOrder inOrder = inOrder(deleteEntityService, entityService);
    inOrder.verify(entityService).deleteUrn(any(), eq(URN), eq(GIVEN));
    inOrder.verify(deleteEntityService).deleteReferencesTo(opContext, URN, REFERRERS);
    verify(deleteEntityService, never()).getGraphReferrers(any(), any());
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), anyBoolean());
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void deleteEntityThenReferencesWithVersionsAndTheFlagOffCleansTheGivenReferrersToo() {
    assertTrue(service(false, null).deleteEntityThenReferences(opContext, URN, GIVEN, REFERRERS));

    final InOrder inOrder = inOrder(deleteEntityService, entityService);
    inOrder.verify(entityService).deleteUrn(any(), eq(URN));
    inOrder.verify(deleteEntityService).deleteReferencesTo(opContext, URN, REFERRERS);
  }

  /** PARTIAL: the entity stays, nothing is cleaned. */
  @Test
  public void deleteEntityThenReferencesWithVersionsPartialSkipsTheReferences() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    assertFalse(service(true, null).deleteEntityThenReferences(opContext, URN, GIVEN, REFERRERS));

    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), anyList());
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), anyBoolean());
  }

  /**
   * Run again once the entity is gone (the first run failed after its delete): nothing is deleted
   * again and the referrers read before the first delete are still cleaned.
   */
  @Test
  public void deleteEntityThenReferencesWithVersionsOfAGoneEntityCleansTheGivenReferrers() {
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.empty());

    assertTrue(service(true, null).deleteEntityThenReferences(opContext, URN, GIVEN, REFERRERS));

    verify(entityService, never()).deleteUrn(any(), eq(URN), any(DeleteCeiling.class));
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, REFERRERS);
  }

  @Test
  public void deleteEntityAndTimeseriesWithVersionsDeletesTheWindow() {
    assertTrue(
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, URN, GIVEN, ASPECTS, START, END));

    verify(entityService).deleteUrn(any(), eq(URN), eq(GIVEN));
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
    verifyNoInteractions(dispatcher);
  }

  @Test
  public void deleteEntityAndTimeseriesWithVersionsPartialSkipsTheTimeseries() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(aspectDeleted(URN));

    assertFalse(
        service(true, null).deleteEntityAndTimeseries(opContext, URN, GIVEN, ASPECTS, START, END));

    verifyNoInteractions(timeseriesAspectService);
  }

  private HardDeleteService service(
      final boolean reliableHardDelete, final HardDeleteDispatcher hardDeleteDispatcher) {
    return new HardDeleteService(
        entityService,
        deleteEntityService,
        timeseriesAspectService,
        new ReliableHardDelete(entityService, reliableHardDelete),
        hardDeleteDispatcher);
  }

  /** Today's cleanup: the graph is read when the cleanup runs, not before the delete. */
  private void verifyGraphReferrersNotReadAhead() {
    verify(deleteEntityService, never()).getGraphReferrers(any(), any());
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), anyList());
  }

  private void verifyNoDelete() {
    verify(entityService, never()).deleteUrn(any(), any());
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  private static Filter windowFilter() {
    return QueryUtils.getFilterFromCriteria(
        List.of(
            CriterionUtils.buildCriterion("urn", Condition.EQUAL, URN.toString()),
            CriterionUtils.buildCriterion(
                "timestampMillis", Condition.GREATER_THAN_OR_EQUAL_TO, String.valueOf(START)),
            CriterionUtils.buildCriterion(
                "timestampMillis", Condition.LESS_THAN_OR_EQUAL_TO, String.valueOf(END))));
  }

  /** The key was deleted: the entity is gone. */
  private static RollbackRunResult keyDeleted(final Urn urn, final int rows) {
    return new RollbackRunResult(
        List.of(),
        rows,
        List.of(
            new RollbackResult(
                urn, "dataset", "datasetKey", null, null, null, null, ChangeType.DELETE, true, 0)));
  }

  /** Only a non-key aspect was deleted: the entity stays. */
  private static RollbackRunResult aspectDeleted(final Urn urn) {
    return new RollbackRunResult(
        List.of(),
        0,
        List.of(
            new RollbackResult(
                urn,
                "dataset",
                "datasetProperties",
                null,
                null,
                null,
                null,
                ChangeType.DELETE,
                false,
                0)));
  }
}
