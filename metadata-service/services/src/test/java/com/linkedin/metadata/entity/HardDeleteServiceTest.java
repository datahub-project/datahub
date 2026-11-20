package com.linkedin.metadata.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
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
import com.linkedin.metadata.query.filter.Condition;
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
  private static final List<String> ASPECTS = List.of("datasetProfile");
  private static final List<String> TWO_ASPECTS = List.of("datasetProfile", "operation");
  private static final long START = 10L;
  private static final long END = 20L;

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
    // offer() reads dispatch(), as the interface's default does, unless a test says otherwise.
    when(dispatcher.offer(any(), any())).thenCallRealMethod();
    referencesDeleted =
        new DeleteReferencesResponse().setTotal(2).setRelatedAspects(new RelatedAspectArray());
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CAPTURED));
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(keyDeleted(URN, 5));
    when(entityService.deleteUrn(any(), eq(URN))).thenReturn(keyDeleted(URN, 6));
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), anyBoolean()))
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
    inOrder
        .verify(dispatcher)
        .dispatchOneOfMany(opContext, HardDeleteRequest.entity(URN, CAPTURED));
    inOrder.verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    inOrder.verify(dispatcher).dispatchOneOfMany(opContext, HardDeleteRequest.entity(OTHER, other));
    inOrder.verify(entityService).deleteUrn(any(), eq(OTHER), eq(other));
    verify(dispatcher, never()).dispatch(any(), any());
  }

  /** A request with one urn is offered as a single delete, not as one of many. */
  @Test
  public void deleteEntitiesOfOneUrnIsOfferedAsASingleDelete() {
    service(true, dispatcher).deleteEntities(opContext, List.of(URN));

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(URN, CAPTURED));
    verify(dispatcher, never()).dispatchOneOfMany(any(), any());
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
  }

  @Test
  public void deleteEntityThenReferencesWithTheFlagOffDeletesUnbounded() {
    service(false, null).deleteEntityThenReferences(opContext, URN, Supplier::get).run();

    verify(entityService).deleteUrn(any(), eq(URN));
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
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
    verify(entityService, never()).countUrnRows(any(), any());
    verify(timeseriesAspectService, never()).countByFilter(any(), any(), any(), any());
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
  public void deleteEntityAndTimeseriesTakenRunsNothingHereAndReportsWhatIsThere() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);
    final OperationContext timeseriesContext = mock(OperationContext.class);
    when(timeseriesAspectService.countByFilter(
            eq(timeseriesContext), eq("dataset"), eq("datasetProfile"), eq(windowFilter())))
        .thenReturn(3L);
    when(timeseriesAspectService.countByFilter(
            eq(timeseriesContext), eq("dataset"), eq("operation"), eq(windowFilter())))
        .thenReturn(5L);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, timeseriesContext, URN, TWO_ASPECTS, START, END);

    verify(dispatcher)
        .dispatch(
            opContext,
            HardDeleteRequest.entityAndTimeseries(URN, CAPTURED, TWO_ASPECTS, START, END));
    assertEquals(response.getRows(), Long.valueOf(9L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(8L));
    assertTrue(response.hasQueued() && response.isQueued());
    verifyNoDelete();
    verify(timeseriesAspectService, never()).deleteAspectValues(any(), any(), any(), any());
    // The count runs under the caller's timeseries authz context, as the delete does.
    verify(timeseriesAspectService, never())
        .countByFilter(eq(opContext), anyString(), anyString(), any());
  }

  /** The delete is already submitted: a failed count is -1, never a failed request. */
  @Test
  public void deleteEntityAndTimeseriesTakenStillAnswersWhenACountFails() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(entityService.countUrnRows(any(), eq(URN))).thenThrow(new IllegalStateException("db"));
    stubTimeseriesCount("datasetProfile", 3L);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    assertEquals(response.getRows(), Long.valueOf(-1L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(3L));
    assertTrue(response.isQueued());
  }

  @Test
  public void deleteEntityAndTimeseriesTakenAnswersWhenTheTimeseriesCountFails() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);
    when(timeseriesAspectService.countByFilter(any(), anyString(), anyString(), any()))
        .thenThrow(new IllegalStateException("es"));

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    assertEquals(response.getRows(), Long.valueOf(9L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(-1L));
    assertTrue(response.isQueued());
  }

  /** A DAO that cannot count (Cassandra) answers -1; it passes through. */
  @Test
  public void deleteEntityAndTimeseriesTakenPassesAnUnsupportedRowCountThrough() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(-1);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, List.of(), START, END);

    assertEquals(response.getRows(), Long.valueOf(-1L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(0L));
    verifyNoInteractions(timeseriesAspectService);
  }

  /** The count reads the window the delete deletes, so the two cannot drift apart. */
  @Test
  public void deleteEntityAndTimeseriesTakenCountsTheSameWindowItDeletes() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    service(true, dispatcher)
        .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);
    service(true, null).deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(timeseriesAspectService)
        .countByFilter(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
  }

  /** Declined: today's delete runs here and reports its own counts, not the ones read first. */
  @Test
  public void deleteEntityAndTimeseriesDeclinedRunsTodaysCodeAfterOneOffer() {
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(dispatcher, times(1)).dispatch(any(), any());
    verify(entityService).deleteUrn(any(), eq(URN), eq(CAPTURED));
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), eq(windowFilter()));
    assertEquals(response.getRows(), Long.valueOf(5L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(4L));
  }

  /**
   * A delete the dispatcher runs itself is gone when the offer returns, so the counts are read
   * before it: they are what the delete removes. Done here is not queued.
   */
  @Test
  public void deleteEntityAndTimeseriesDoneByTheDispatcherReportsWhatItRemovedWithoutQueued() {
    doReturn(HardDeleteDispatcher.Offer.of(HardDeleteDispatcher.Outcome.DONE))
        .when(dispatcher)
        .offer(any(), any());
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);
    when(timeseriesAspectService.countByFilter(any(), anyString(), anyString(), any()))
        .thenReturn(30L);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    final InOrder inOrder = inOrder(entityService, timeseriesAspectService, dispatcher);
    inOrder.verify(entityService).countUrnRows(opContext, URN);
    inOrder.verify(timeseriesAspectService).countByFilter(any(), anyString(), anyString(), any());
    inOrder.verify(dispatcher).offer(any(), any());
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
    assertEquals(response.getRows(), Long.valueOf(9L));
    assertEquals(response.getTimeseriesRows(), Long.valueOf(30L));
    assertFalse(response.hasQueued());
  }

  /** Queued: the same counts, read before the job can start, and queued is set. */
  @Test
  public void deleteEntityAndTimeseriesQueuedCountsBeforeTheOffer() {
    doReturn(HardDeleteDispatcher.Offer.of(HardDeleteDispatcher.Outcome.QUEUED))
        .when(dispatcher)
        .offer(any(), any());
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);

    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    final InOrder inOrder = inOrder(entityService, dispatcher);
    inOrder.verify(entityService).countUrnRows(opContext, URN);
    inOrder.verify(dispatcher).offer(any(), any());
    assertEquals(response.getRows(), Long.valueOf(9L));
    assertTrue(response.isQueued());
  }

  @Test
  public void deleteEntityAndTimeseriesDeclinedDoesNotReportQueued() {
    final DeleteEntityResponse response =
        service(true, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    assertFalse(response.hasQueued());
  }

  @Test
  public void deleteEntityAndTimeseriesWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(entityService.countUrnRows(any(), eq(URN))).thenReturn(9);

    final DeleteEntityResponse response =
        service(false, dispatcher)
            .deleteEntityAndTimeseries(opContext, opContext, URN, ASPECTS, START, END);

    verify(dispatcher, times(1)).dispatch(any(), any());
    verifyNoDelete();
    assertEquals(response.getRows(), Long.valueOf(9L));
    assertTrue(response.isQueued());
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
    // Counted before the offer, but never deleted.
    verify(timeseriesAspectService, never()).deleteAspectValues(any(), any(), any(), any());
  }

  // ---- deleteReferences(op, urn)

  @Test
  public void deleteReferencesWithoutDispatcherRunsTodaysCode() {
    assertSame(service(true, null).deleteReferences(opContext, URN), referencesDeleted);
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), eq(true));
  }

  @Test
  public void deleteReferencesTakenRunsNothingHereAndReportsWhatIsThere() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    final DeleteReferencesResponse dryRun =
        new DeleteReferencesResponse().setTotal(7).setRelatedAspects(new RelatedAspectArray());
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true))).thenReturn(dryRun);

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.references(URN));
    verify(deleteEntityService, times(1)).deleteReferencesTo(opContext, URN, true);
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), eq(false));
    assertEquals(response.getTotal(), Integer.valueOf(7));
    assertTrue(response.isQueued());
  }

  @Test
  public void deleteReferencesTakenStillAnswersWhenTheDryRunFails() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenThrow(new IllegalStateException("graph"));

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    assertEquals(response.getTotal(), Integer.valueOf(-1));
    assertTrue(response.getRelatedAspects().isEmpty());
    assertTrue(response.isQueued());
  }

  @Test
  public void deleteReferencesDeclinedRunsTodaysCodeAfterOneOffer() {
    assertSame(service(true, dispatcher).deleteReferences(opContext, URN), referencesDeleted);

    verify(dispatcher, times(1)).dispatch(opContext, HardDeleteRequest.references(URN));
    verify(deleteEntityService).deleteReferencesTo(opContext, URN, false);
  }

  /** A cleanup the dispatcher runs itself reports the dry run read before it, without queued. */
  @Test
  public void deleteReferencesDoneByTheDispatcherReportsTheDryRunWithoutQueued() {
    doReturn(HardDeleteDispatcher.Offer.of(HardDeleteDispatcher.Outcome.DONE))
        .when(dispatcher)
        .offer(any(), any());
    final DeleteReferencesResponse dryRun =
        new DeleteReferencesResponse().setTotal(7).setRelatedAspects(new RelatedAspectArray());
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true))).thenReturn(dryRun);

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    final InOrder inOrder = inOrder(deleteEntityService, dispatcher);
    inOrder.verify(deleteEntityService).deleteReferencesTo(opContext, URN, true);
    inOrder.verify(dispatcher).offer(opContext, HardDeleteRequest.references(URN));
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), eq(false));
    assertEquals(response.getTotal(), Integer.valueOf(7));
    assertFalse(response.hasQueued());
  }

  /**
   * The CLI's second call, after its entity delete dropped the graph edges: the dry run reads the
   * graph and finds nothing, but the dispatcher counted the referrers the delete kept. The larger
   * count is reported.
   */
  @Test
  public void deleteReferencesReportsTheDispatchersCountWhenTheGraphNoLongerHasTheReferrers() {
    doReturn(new HardDeleteDispatcher.Offer(HardDeleteDispatcher.Outcome.QUEUED, 8))
        .when(dispatcher)
        .offer(any(), any());
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(0).setRelatedAspects(new RelatedAspectArray()));

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    assertEquals(response.getTotal(), Integer.valueOf(8));
    assertTrue(response.isQueued());
  }

  /** The dry run's count wins when it is larger, e.g. a capped routing count. */
  @Test
  public void deleteReferencesKeepsTheDryRunWhenItCountsMore() {
    doReturn(new HardDeleteDispatcher.Offer(HardDeleteDispatcher.Outcome.DONE, 3))
        .when(dispatcher)
        .offer(any(), any());
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(7).setRelatedAspects(new RelatedAspectArray()));

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    assertEquals(response.getTotal(), Integer.valueOf(7));
    assertFalse(response.hasQueued());
  }

  /** A failed dry run (-1) still reports the dispatcher's count. */
  @Test
  public void deleteReferencesReportsTheDispatchersCountWhenTheDryRunFails() {
    doReturn(new HardDeleteDispatcher.Offer(HardDeleteDispatcher.Outcome.QUEUED, 8))
        .when(dispatcher)
        .offer(any(), any());
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenThrow(new IllegalStateException("graph"));

    final DeleteReferencesResponse response =
        service(true, dispatcher).deleteReferences(opContext, URN);

    assertEquals(response.getTotal(), Integer.valueOf(8));
    assertTrue(response.isQueued());
  }

  @Test
  public void deleteReferencesDeclinedDoesNotReportQueued() {
    assertFalse(service(true, dispatcher).deleteReferences(opContext, URN).hasQueued());
  }

  @Test
  public void deleteReferencesWithTheFlagOffIsStillOffered() {
    when(dispatcher.dispatch(any(), any())).thenReturn(true);
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(7).setRelatedAspects(new RelatedAspectArray()));

    final DeleteReferencesResponse response =
        service(false, dispatcher).deleteReferences(opContext, URN);

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.references(URN));
    verify(deleteEntityService, times(1)).deleteReferencesTo(opContext, URN, true);
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), eq(false));
    assertEquals(response.getTotal(), Integer.valueOf(7));
    assertTrue(response.isQueued());
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

  private void stubTimeseriesCount(final String aspect, final long count) {
    when(timeseriesAspectService.countByFilter(
            any(), eq("dataset"), eq(aspect), eq(windowFilter())))
        .thenReturn(count);
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
