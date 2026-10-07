package com.linkedin.metadata.service.async.delete;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCascadeListener;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReliableHardDeleteTest {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final Urn OTHER_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.other_table,PROD)");
  private static final long NOW = 1_700_000_000_000L;
  private static final DeleteCeiling CEILING =
      new DeleteCeiling(Map.of("status", 3L, "datasetProperties", 7L), 1_000L, NOW);
  private static final long DOCS_PER_ASPECT = 2L;

  private final OperationContext actorContext =
      TestOperationContexts.userContextNoSearchAuthorization(
          UrnUtils.getUrn("urn:li:corpuser:alice"));
  private EntityService<?> entityService;
  private DeleteEntityService deleteEntityService;
  private TimeseriesAspectService timeseriesAspectService;
  private GraphService graphService;
  private ReliableHardDelete reliableHardDelete;
  private long timeseriesDocs;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    graphService = mock(GraphService.class);
    reliableHardDelete =
        new ReliableHardDelete(
            entityService,
            deleteEntityService,
            timeseriesAspectService,
            graphService,
            Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC),
            true);

    when(entityService.captureDeleteCeiling(any(), eq(URN), eq(NOW)))
        .thenReturn(Optional.of(CEILING));
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(deleted(ConditionalDeleteOutcome.DELETED, 9));
    when(deleteEntityService.removeReferencesResumable(any(), eq(URN), any(), any())).thenReturn(4);
    when(timeseriesAspectService.deleteAspectValues(any(), anyString(), anyString(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(DOCS_PER_ASPECT));
    timeseriesDocs =
        DOCS_PER_ASPECT
            * actorContext
                .getEntityRegistry()
                .getEntitySpec(URN.getEntityType())
                .getAspectSpecs()
                .stream()
                .filter(AspectSpec::isTimeseries)
                .count();
  }

  /** An absent entity still gets its graph node and timeseries removed: idempotent leftovers. */
  @Test
  public void absentEntityRemovesLeftoversAndReportsAlreadyDeleted() {
    when(entityService.captureDeleteCeiling(any(), eq(URN), eq(NOW))).thenReturn(Optional.empty());

    assertEquals(
        reliableHardDelete.delete(actorContext, URN),
        new DeleteEntityReport(
            URN.toString(), ConditionalDeleteOutcome.ALREADY_DELETED, 0L, timeseriesDocs, 0));
    verify(graphService).removeNodeReportingFailures(any(), eq(URN), eq(true));
    verifyNoInteractions(deleteEntityService);
    verify(entityService, never()).validateHardDelete(any(), any());
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  /**
   * The first request deletes the rows but fails removing the graph node; the retry finds the key
   * gone and finishes the cleanup.
   */
  @Test
  public void aRetryAfterAFailedCleanupFinishesIt() {
    doThrow(new IllegalStateException("1 version conflict"))
        .doNothing()
        .when(graphService)
        .removeNodeReportingFailures(any(), eq(URN), anyBoolean());

    expectThrows(IllegalStateException.class, () -> reliableHardDelete.delete(actorContext, URN));
    verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
    verifyNoInteractions(timeseriesAspectService);

    when(entityService.captureDeleteCeiling(any(), eq(URN), eq(NOW))).thenReturn(Optional.empty());
    final DeleteEntityReport retry = reliableHardDelete.delete(actorContext, URN);

    assertEquals(retry.outcome(), ConditionalDeleteOutcome.ALREADY_DELETED);
    assertEquals(retry.timeseriesRowsDeleted(), timeseriesDocs);
    verify(graphService, times(2)).removeNodeReportingFailures(any(), eq(URN), eq(true));
    verify(entityService, times(1)).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  @Test
  public void aFailedLeftoverCleanupFailsTheRequestWithItsCause() {
    when(entityService.captureDeleteCeiling(any(), eq(URN), eq(NOW))).thenReturn(Optional.empty());
    final RuntimeException searchDown = new RuntimeException("search unavailable");
    doThrow(searchDown)
        .when(graphService)
        .removeNodeReportingFailures(any(), eq(URN), anyBoolean());

    final IllegalStateException thrown =
        expectThrows(
            IllegalStateException.class, () -> reliableHardDelete.delete(actorContext, URN));

    assertSame(thrown.getCause(), searchDown);
  }

  @Test
  public void rejectedByValidatorsChangesNothing() {
    final IllegalArgumentException rejected = new IllegalArgumentException("soft-delete it first");
    doThrow(rejected).when(entityService).validateHardDelete(any(), eq(URN));

    final IllegalArgumentException thrown =
        expectThrows(
            IllegalArgumentException.class, () -> reliableHardDelete.delete(actorContext, URN));

    assertSame(thrown, rejected);
    verifyNoInteractions(deleteEntityService, graphService, timeseriesAspectService);
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  /** References first, from the first phase, then the delete bounded by the captured ceiling. */
  @Test
  public void removesReferencesThenDeletesUpToTheCapturedCeiling() {
    final DeleteEntityReport report = reliableHardDelete.delete(actorContext, URN);

    assertEquals(
        report,
        new DeleteEntityReport(
            URN.toString(), ConditionalDeleteOutcome.DELETED, 9L, timeseriesDocs, 4));
    final InOrder order = inOrder(deleteEntityService, entityService, graphService);
    order
        .verify(deleteEntityService)
        .removeReferencesResumable(any(), eq(URN), isNull(), eq(DeleteCascadeListener.NOOP));
    order.verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
    order.verify(graphService).removeNodeReportingFailures(any(), eq(URN), eq(true));
  }

  @Test
  public void partialDeleteKeepsTheNodeAndDeletesTimeseries() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(deleted(ConditionalDeleteOutcome.PARTIAL, 5));

    final DeleteEntityReport report = reliableHardDelete.delete(actorContext, URN);

    assertEquals(report.outcome(), ConditionalDeleteOutcome.PARTIAL);
    assertEquals(report.timeseriesRowsDeleted(), timeseriesDocs);
    verify(graphService, never()).removeNodeReportingFailures(any(), any(), anyBoolean());
  }

  /** Present at capture, recreated (or deleted by someone else) before the delete: no cleanup. */
  @Test
  public void aConcurrentDeleteLeavesTheRestToIt() {
    when(entityService.deleteUrn(any(), eq(URN), any(DeleteCeiling.class)))
        .thenReturn(deleted(ConditionalDeleteOutcome.ALREADY_DELETED, 0));

    final DeleteEntityReport report = reliableHardDelete.delete(actorContext, URN);

    assertEquals(report.outcome(), ConditionalDeleteOutcome.ALREADY_DELETED);
    verifyNoInteractions(graphService, timeseriesAspectService);
  }

  @Test
  public void aReferenceThatCannotBeRemovedFailsBeforeAnyDelete() {
    when(deleteEntityService.removeReferencesResumable(any(), eq(URN), any(), any()))
        .thenThrow(new IllegalStateException("Write of domains to a referrer was not committed"));

    expectThrows(IllegalStateException.class, () -> reliableHardDelete.delete(actorContext, URN));

    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
    verifyNoInteractions(graphService, timeseriesAspectService);
  }

  @Test
  public void anIncompleteNodeRemovalFails() {
    doThrow(new IllegalStateException("1 version conflict"))
        .when(graphService)
        .removeNodeReportingFailures(any(), eq(URN), anyBoolean());

    expectThrows(IllegalStateException.class, () -> reliableHardDelete.delete(actorContext, URN));
  }

  /** A store failure mid-delete fails the request; the cause is kept for the server log. */
  @Test
  public void aFailureDuringTheDeleteFailsTheRequestWithItsCause() {
    final RuntimeException storeDown = new RuntimeException("search unavailable");
    when(deleteEntityService.removeReferencesResumable(any(), eq(URN), any(), any()))
        .thenThrow(storeDown);

    final IllegalStateException thrown =
        expectThrows(
            IllegalStateException.class, () -> reliableHardDelete.delete(actorContext, URN));

    assertSame(thrown.getCause(), storeDown);
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
  }

  /** Timeseries documents at or before the capture time go; later ones and other urns' stay. */
  @Test
  public void deletesOnlyTimeseriesDocumentsAtOrBeforeTheCapture() {
    final List<TimeseriesDoc> docs =
        new ArrayList<>(
            List.of(
                new TimeseriesDoc(URN.toString(), "datasetProfile", NOW - 1),
                new TimeseriesDoc(URN.toString(), "datasetProfile", NOW),
                new TimeseriesDoc(URN.toString(), "datasetProfile", NOW + 1),
                new TimeseriesDoc(URN.toString(), "datasetUsageStatistics", NOW),
                new TimeseriesDoc(OTHER_URN.toString(), "datasetProfile", NOW - 1)));
    when(timeseriesAspectService.deleteAspectValues(any(), anyString(), anyString(), any()))
        .thenAnswer(
            invocation ->
                TimeseriesDoc.deleteMatching(
                    docs, invocation.getArgument(2), invocation.getArgument(3)));

    final DeleteEntityReport report = reliableHardDelete.delete(actorContext, URN);

    assertEquals(report.timeseriesRowsDeleted(), 3L);
    assertEquals(
        docs,
        List.of(
            new TimeseriesDoc(URN.toString(), "datasetProfile", NOW + 1),
            new TimeseriesDoc(OTHER_URN.toString(), "datasetProfile", NOW - 1)));
  }

  /** A timeseries document; {@link #deleteMatching} applies a delete filter as the index would. */
  private record TimeseriesDoc(String urn, String aspect, long timestampMillis) {
    static DeleteAspectValuesResult deleteMatching(
        final List<TimeseriesDoc> docs, final String aspect, final Filter filter) {
      final List<Criterion> criteria = filter.getOr().get(0).getAnd();
      final Predicate<TimeseriesDoc> matches =
          doc -> doc.aspect().equals(aspect) && criteria.stream().allMatch(doc::holds);
      final long deleted = docs.stream().filter(matches).count();
      docs.removeIf(matches);
      return new DeleteAspectValuesResult().setNumDocsDeleted(deleted);
    }

    boolean holds(final Criterion criterion) {
      final String value = criterion.getValues().get(0);
      if ("urn".equals(criterion.getField()) && criterion.getCondition() == Condition.EQUAL) {
        return urn.equals(value);
      }
      if ("timestampMillis".equals(criterion.getField())
          && criterion.getCondition() == Condition.LESS_THAN_OR_EQUAL_TO) {
        return timestampMillis <= Long.parseLong(value);
      }
      throw new AssertionError("unexpected criterion " + criterion);
    }
  }

  private static RollbackRunResult deleted(final ConditionalDeleteOutcome outcome, final int rows) {
    return new RollbackRunResult(List.of(), rows, List.of(), outcome);
  }
}
