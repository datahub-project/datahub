package com.linkedin.metadata.elasticsearch.update;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.search.elasticsearch.update.BulkItemRequeueSupport;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.search.elasticsearch.update.BulkWriteResultTracker;
import java.util.Collections;
import java.util.function.Consumer;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.core.rest.RestStatus;
import org.testng.annotations.Test;

public class BulkListenerFailureHandlingTest {

  @Test
  public void testSuccessCompletesPending() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, null);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);
    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(false);
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.OK);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(false);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));

    listener.afterBulk(1L, bulkRequest, response);
    assertEquals(tracker.getPendingItems(), 0);
  }

  @Test
  public void testVersionConflictRequeue() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.CONFLICT);
    when(failure.getMessage()).thenReturn("version_conflict_engine_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("version_conflict_engine_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.CONFLICT);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("conflict");

    listener.afterBulk(1L, bulkRequest, response);

    verify(requeue, times(1)).accept(any(DocWriteRequest.class));
    // still pending while requeued
    assertEquals(tracker.getPendingItems(), 1);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testExhaustedConflictIsLwwNotTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    // maxAttempts 0 disables requeue
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 0, req -> {});
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.CONFLICT);
    when(failure.getMessage()).thenReturn("version_conflict_engine_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("version_conflict_engine_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.CONFLICT);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("conflict");

    listener.afterBulk(1L, bulkRequest, response);
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 0);
  }

  @Test
  public void testNonRetriableIsTransferFailure() {
    BulkWriteResultTracker tracker = new BulkWriteResultTracker();
    tracker.recordEnqueued(1);
    Consumer<DocWriteRequest<?>> requeue = mock(Consumer.class);
    BulkItemRequeueSupport support = new BulkItemRequeueSupport(true, 3, requeue);
    BulkListener listener =
        BulkListener.create(WriteRequest.RefreshPolicy.NONE, null, tracker, support);

    IndexRequest indexRequest = new IndexRequest("idx").id("1").source(Collections.emptyMap());
    BulkRequest bulkRequest = new BulkRequest().add(indexRequest);

    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(RestStatus.BAD_REQUEST);
    when(failure.getMessage()).thenReturn("mapper_parsing_exception");

    BulkItemResponse item = mock(BulkItemResponse.class);
    when(item.isFailed()).thenReturn(true);
    when(item.getFailure()).thenReturn(failure);
    when(item.getFailureMessage()).thenReturn("mapper_parsing_exception");
    when(item.getOpType()).thenReturn(DocWriteRequest.OpType.INDEX);
    when(item.status()).thenReturn(RestStatus.BAD_REQUEST);

    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);
    when(response.getItems()).thenReturn(new BulkItemResponse[] {item});
    when(response.getIngestTookInMillis()).thenReturn(BulkResponse.NO_INGEST_TOOK);
    when(response.getTook()).thenReturn(org.opensearch.common.unit.TimeValue.timeValueMillis(1));
    when(response.buildFailureMessage()).thenReturn("mapper");

    listener.afterBulk(1L, bulkRequest, response);
    verify(requeue, never()).accept(any());
    assertEquals(tracker.getPendingItems(), 0);
    assertEquals(tracker.getUnrecoveredTransferFailures(), 1);
  }
}
