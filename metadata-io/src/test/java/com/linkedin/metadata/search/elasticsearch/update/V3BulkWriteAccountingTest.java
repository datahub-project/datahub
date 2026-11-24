package com.linkedin.metadata.search.elasticsearch.update;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class V3BulkWriteAccountingTest {

  @BeforeMethod
  public void setup() {
    V3BulkWriteAccounting.resetForTests();
  }

  @Test
  public void testClassifiesV3StaleConflict() {
    assertTrue(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3", "INDEX", RestStatus.CONFLICT.getStatus()));
    assertTrue(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3_1683649932260", "INDEX", RestStatus.CONFLICT.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "datasetindex_v2", "INDEX", RestStatus.CONFLICT.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "datasetindex_v30", "INDEX", RestStatus.CONFLICT.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3", "DELETE", RestStatus.CONFLICT.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3", "INDEX", RestStatus.INTERNAL_SERVER_ERROR.getStatus()));
  }

  @Test
  public void testACreateOverAnExistingDocumentAndAMissingUpdateAreExpected() {
    assertTrue(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3", "CREATE", RestStatus.CONFLICT.getStatus()));
    assertTrue(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "primaryindex_v3", "CREATE", RestStatus.CREATED.getStatus()));
    assertTrue(
        V3BulkWriteAccounting.isExpectedV3MissingDocument(
            "primaryindex_v3", "UPDATE", RestStatus.NOT_FOUND.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3MissingDocument(
            "datasetindex_v2", "UPDATE", RestStatus.NOT_FOUND.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3MissingDocument(
            "primaryindex_v3", "INDEX", RestStatus.NOT_FOUND.getStatus()));
    // A live merge that conflicts is not a create the fill expects to lose.
    assertFalse(
        V3BulkWriteAccounting.isExpectedV3StaleConflict(
            "primaryindex_v3", "UPDATE", RestStatus.CONFLICT.getStatus()));
  }

  @Test
  public void testASuccessfulV3UpdateCountsAsAWrite() {
    assertTrue(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "primaryindex_v3", "UPDATE", RestStatus.OK.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "primaryindex_v3", "UPDATE", RestStatus.NOT_FOUND.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "datasetindex_v2", "UPDATE", RestStatus.OK.getStatus()));
  }

  @Test
  public void testAV3DeleteCountsAsAWriteAndAMissingOneIsExpected() {
    assertTrue(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "primaryindex_v3", "DELETE", RestStatus.OK.getStatus()));
    assertTrue(
        V3BulkWriteAccounting.isExpectedV3MissingDocument(
            "primaryindex_v3", "DELETE", RestStatus.NOT_FOUND.getStatus()));
    assertFalse(
        V3BulkWriteAccounting.isSuccessfulV3IndexWrite(
            "datasetindex_v2", "DELETE", RestStatus.OK.getStatus()));
  }

  @Test
  public void testSnapshotDeltaTracksCounters() {
    V3BulkWriteAccounting.Snapshot before = V3BulkWriteAccounting.snapshot();

    V3BulkWriteAccounting.recordSuccessfulV3Writes(2, null);
    V3BulkWriteAccounting.recordStaleVersionRejected(1, null);

    V3BulkWriteAccounting.Snapshot delta = V3BulkWriteAccounting.snapshot().deltaSince(before);
    assertEquals(delta.successfulV3Writes(), 2);
    assertEquals(delta.staleVersionRejected(), 1);
  }

  @Test
  public void testZeroOrNegativeCountsDoNotRecordMetrics() {
    MetricUtils metricUtils = mock(MetricUtils.class);

    V3BulkWriteAccounting.recordSuccessfulV3Writes(0, metricUtils);
    V3BulkWriteAccounting.recordStaleVersionRejected(-1, metricUtils);

    V3BulkWriteAccounting.Snapshot snapshot = V3BulkWriteAccounting.snapshot();
    assertEquals(snapshot.successfulV3Writes(), 0);
    assertEquals(snapshot.staleVersionRejected(), 0);
    verifyNoInteractions(metricUtils);
  }

  @Test
  public void testRecordOpenSearchResponseCountsV3Outcomes() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkItemResponse success = mock(BulkItemResponse.class);
    BulkItemResponse stale = mock(BulkItemResponse.class);
    BulkItemResponse unexpected = mock(BulkItemResponse.class);
    BulkResponse response = mock(BulkResponse.class);

    when(success.getIndex()).thenReturn("primaryindex_v3_1683649932260");
    when(success.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(success.status()).thenReturn(RestStatus.CREATED);
    when(success.isFailed()).thenReturn(false);

    when(stale.getIndex()).thenReturn("primaryindex_v3");
    when(stale.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(stale.status()).thenReturn(RestStatus.CONFLICT);
    when(stale.isFailed()).thenReturn(true);

    when(unexpected.getIndex()).thenReturn("datasetindex_v2");
    when(unexpected.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(unexpected.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);
    when(unexpected.isFailed()).thenReturn(true);

    when(response.getItems()).thenReturn(new BulkItemResponse[] {success, stale, unexpected});

    V3BulkWriteAccounting.BulkWriteOutcome outcome =
        V3BulkWriteAccounting.recordOpenSearchResponse(response, metricUtils);

    assertEquals(outcome.successfulV3Writes(), 1);
    assertEquals(outcome.staleVersionRejected(), 1);
    assertEquals(outcome.unexpectedFailures(), 1);
    verify(metricUtils)
        .increment(BulkListener.class, V3BulkWriteAccounting.SUCCESSFUL_WRITES_METRIC, 1d);
    verify(metricUtils)
        .increment(BulkListener.class, V3BulkWriteAccounting.STALE_VERSION_REJECTED_METRIC, 1d);
  }

  @Test
  public void testRecordEs8ResponseCountsV3Outcomes() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkResponseItem success =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3_1683649932260")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(201));
    BulkResponseItem stale =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3")
                    .id("urn-2")
                    .operationType(OperationType.Index)
                    .status(409)
                    .error(versionConflict()));
    BulkResponseItem unexpected =
        BulkResponseItem.of(
            b ->
                b.index("datasetindex_v2")
                    .id("urn-3")
                    .operationType(OperationType.Index)
                    .status(500)
                    .error(ErrorCause.of(e -> e.type("internal_server_error").reason("failed"))));
    co.elastic.clients.elasticsearch.core.BulkResponse response =
        co.elastic.clients.elasticsearch.core.BulkResponse.of(
            b -> b.errors(true).took(9).items(success, stale, unexpected));

    V3BulkWriteAccounting.BulkWriteOutcome outcome =
        V3BulkWriteAccounting.recordEs8Response(response, metricUtils);

    assertEquals(outcome.successfulV3Writes(), 1);
    assertEquals(outcome.staleVersionRejected(), 1);
    assertEquals(outcome.unexpectedFailures(), 1);
    verify(metricUtils)
        .increment(BulkListener.class, V3BulkWriteAccounting.SUCCESSFUL_WRITES_METRIC, 1d);
    verify(metricUtils)
        .increment(BulkListener.class, V3BulkWriteAccounting.STALE_VERSION_REJECTED_METRIC, 1d);
  }

  @Test
  public void testFillConflictsAndMissingLiveMergesLeaveOnlyExpectedFailures() {
    BulkItemResponse filledAlready = mock(BulkItemResponse.class);
    when(filledAlready.getIndex()).thenReturn("primaryindex_v3");
    when(filledAlready.getOpType()).thenReturn(DocWriteRequest.OpType.CREATE);
    when(filledAlready.status()).thenReturn(RestStatus.CONFLICT);
    when(filledAlready.isFailed()).thenReturn(true);
    BulkItemResponse notFilledYet = mock(BulkItemResponse.class);
    when(notFilledYet.getIndex()).thenReturn("primaryindex_v3");
    when(notFilledYet.getOpType()).thenReturn(DocWriteRequest.OpType.UPDATE);
    when(notFilledYet.status()).thenReturn(RestStatus.NOT_FOUND);
    when(notFilledYet.isFailed()).thenReturn(true);
    BulkResponse response = mock(BulkResponse.class);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {filledAlready, notFilledYet});

    V3BulkWriteAccounting.BulkWriteOutcome outcome =
        V3BulkWriteAccounting.recordOpenSearchResponse(response, null);

    assertEquals(outcome.staleVersionRejected(), 1);
    assertEquals(outcome.missingDocumentSkipped(), 1);
    assertTrue(outcome.hasOnlyExpectedFailures());
  }

  private static ErrorCause versionConflict() {
    return ErrorCause.of(e -> e.type("version_conflict_engine_exception").reason("stale"));
  }
}
