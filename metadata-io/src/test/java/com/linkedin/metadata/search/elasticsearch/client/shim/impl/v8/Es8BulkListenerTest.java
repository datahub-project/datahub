package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkProcessorCompletionTracker;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import com.linkedin.metadata.search.elasticsearch.update.V3BulkWriteAccounting;
import com.linkedin.metadata.utils.elasticsearch.BulkProcessorFlushResult;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.action.index.IndexRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Es8BulkListenerTest {

  @BeforeMethod
  public void setup() {
    V3BulkWriteAccounting.resetForTests();
  }

  @Test
  public void testAfterBulkRecordsExpectedV3StaleConflict() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    Es8BulkListener listener =
        new Es8BulkListener(
            metricUtils, tracker, null, null, V3BulkWriteAccounting::classifyEs8Response);
    BulkProcessorCompletionTracker.Submission submission =
        tracker.track(new IndexRequest("primaryindex_v3").id("urn-1"), ignored -> {});
    BulkResponseItem stale =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(409)
                    .error(
                        ErrorCause.of(
                            e -> e.type("version_conflict_engine_exception").reason("stale"))));
    BulkResponse response =
        BulkResponse.of(b -> b.errors(true).took(5).ingestTook(2L).items(stale));

    listener.afterBulk(1L, mock(BulkRequest.class), List.of(submission), response);

    BulkProcessorFlushResult drainResult = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    V3BulkWriteAccounting.Snapshot snapshot = V3BulkWriteAccounting.snapshot();
    assertEquals(snapshot.staleVersionRejected(), 1);
    assertEquals(drainResult.staleVersionRejected(), 1);
    assertFalse(drainResult.hasFailures());
    verify(metricUtils)
        .increment(BulkListener.class, V3BulkWriteAccounting.STALE_VERSION_REJECTED_METRIC, 1d);
  }

  @Test
  public void testGenericListenerDoesNotApplyV3Accounting() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    Es8BulkListener listener = new Es8BulkListener(metricUtils, tracker);
    BulkProcessorCompletionTracker.Submission submission =
        tracker.track(new IndexRequest("primaryindex_v3").id("urn-1"), ignored -> {});
    BulkResponseItem stale =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(409)
                    .error(
                        ErrorCause.of(
                            e -> e.type("version_conflict_engine_exception").reason("stale"))));
    BulkResponse response =
        BulkResponse.of(b -> b.errors(true).took(5).ingestTook(2L).items(stale));

    listener.afterBulk(1L, mock(BulkRequest.class), List.of(submission), response);

    BulkProcessorFlushResult drainResult = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(V3BulkWriteAccounting.snapshot().staleVersionRejected(), 0);
    assertEquals(drainResult.staleVersionRejected(), 1);
    assertFalse(drainResult.hasFailures());
    verify(metricUtils, org.mockito.Mockito.never())
        .increment(BulkListener.class, V3BulkWriteAccounting.STALE_VERSION_REJECTED_METRIC, 1d);
  }

  @Test
  public void testExpectedV3RejectsCompleteTheBulkWait() throws Exception {
    BulkWriteResultTracker resultTracker = new BulkWriteResultTracker();
    resultTracker.recordEnqueued(1);
    Es8BulkListener listener = new Es8BulkListener(null, null, resultTracker, null);
    BulkResponseItem stale =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(409)
                    .error(
                        ErrorCause.of(
                            e -> e.type("version_conflict_engine_exception").reason("stale"))));
    BulkResponse response =
        BulkResponse.of(b -> b.errors(true).took(5).ingestTook(2L).items(stale));

    listener.afterBulk(1L, mock(BulkRequest.class), List.of(), response);

    resultTracker.awaitIdle(Duration.ofSeconds(1));
    assertEquals(resultTracker.getPendingItems(), 0);
    assertEquals(resultTracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testAfterBulkRecordsSuccessfulV3Write() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    Es8BulkListener listener = new Es8BulkListener(null, tracker);
    BulkProcessorCompletionTracker.Submission submission =
        tracker.track(new IndexRequest("primaryindex_v3").id("urn-1"), ignored -> {});
    BulkResponseItem success =
        BulkResponseItem.of(
            b ->
                b.index("primaryindex_v3")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(200));
    BulkResponse response = BulkResponse.of(b -> b.errors(false).took(5).items(success));

    listener.afterBulk(1L, mock(BulkRequest.class), List.of(submission), response);

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.successfulV3Writes(), 1);
    assertFalse(result.hasFailures());
  }

  @Test
  public void testAfterBulkRecordsUnexpectedFailure() {
    MetricUtils metricUtils = mock(MetricUtils.class);
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    Es8BulkListener listener = new Es8BulkListener(metricUtils, tracker);
    BulkProcessorCompletionTracker.Submission submission =
        tracker.track(new IndexRequest("datasetindex_v2").id("urn-1"), ignored -> {});
    BulkResponseItem unexpected =
        BulkResponseItem.of(
            b ->
                b.index("datasetindex_v2")
                    .id("urn-1")
                    .operationType(OperationType.Index)
                    .status(400)
                    .error(
                        ErrorCause.of(
                            e -> e.type("illegal_argument_exception").reason("bad request"))));
    BulkResponse response =
        BulkResponse.of(b -> b.errors(true).took(5).ingestTook(2L).items(unexpected));

    listener.afterBulk(1L, mock(BulkRequest.class), List.of(submission), response);

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.unexpectedItemFailures(), 1);
    assertTrue(result.hasFailures());
  }

  @Test
  public void testAfterBulkRecordsWholeRequestTransportFailure() {
    BulkProcessorCompletionTracker tracker = new BulkProcessorCompletionTracker();
    Es8BulkListener listener = new Es8BulkListener(null, tracker);
    BulkProcessorCompletionTracker.Submission first =
        tracker.track(new IndexRequest("primaryindex_v3").id("urn-1"), ignored -> {});
    BulkProcessorCompletionTracker.Submission second =
        tracker.track(new IndexRequest("primaryindex_v3").id("urn-2"), ignored -> {});

    listener.afterBulk(
        1L,
        mock(BulkRequest.class),
        List.of(first, second),
        new SocketTimeoutException("transport failed"));

    BulkProcessorFlushResult result = tracker.flushAndAwait(() -> {}, 1, TimeUnit.SECONDS);
    assertEquals(result.transportFailedOperations(), 2);
    assertTrue(result.hasFailures());
  }
}
